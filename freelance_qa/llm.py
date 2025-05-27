import csv
import io
import re
from sqlalchemy import Engine, create_engine, text
from sqlalchemy.exc import DatabaseError
from pydantic import BaseModel, Field, AfterValidator
from typing import TypedDict, cast, Annotated
from langchain_core.language_models import BaseChatModel
from langchain_ollama.chat_models import ChatOllama
from langchain_core.prompts import ChatPromptTemplate, MessagesPlaceholder
from langgraph.graph import END, StateGraph, START

from freelance_qa.config import get_config
from freelance_qa.prompts import ANSWER_SYSTEM, GENERATE_SYSTEM, VALIDATE_SYSTEM


class AnswerResults(BaseModel):
    answer: str = Field(description="Ответ на вопрос простым языком")


class QuestionResult(BaseModel):
    question: str = Field(description="Уточняющий вопрос")


def validate_sql_response(value: str):
    res = re.search(r"```sql(.+)```", value)
    if res is not None:
        return res.group(1).strip()
    return value


SQLResponse = Annotated[str, AfterValidator(validate_sql_response)]


class GenerateQuery(BaseModel):
    sql: SQLResponse = Field(description="SQL запрос, который необходимо выполнить для ответа на вопрос")


class GraphState(TypedDict):
    question: str
    current_sql: str
    db_results: list[tuple[str, str]]
    can_answer: bool
    answer: str
    attempts: int


class FreelanceQA:
    FAILED_ANSWER = "Я не могу ответить на вопрос"
    DIALECTS = {
        "sqlite": "SQLite",
        "postgresql": "PostgreSQL"
    }

    def __init__(
        self,
        llm: BaseChatModel | None = None,
        llm_coder: BaseChatModel | None = None,
        max_retries: int = 5,
        db_engine: Engine | None = None,
        retry_on_grader_failure: bool = True
    ) -> None:
        """Класс для ответа на вопросы по БД фрилансеров

        Args:
            llm (BaseChatModel | None, optional): Модель для составления ответа на вопрос по результатам запросов.
                По умолчанию `qwen2.5:7b`.
            llm_coder (BaseChatModel | None, optional): Модель для составления SQL запросов из вопроса пользователя.
                По умолчанию `qwen2.5:7b`.
            max_retries (int, optional): Максимальное количество попыток исполнить запрос к БД. Defaults to 5.
            db_engine (Engine | None, optional): БД SQLAlchemy. Defaults to None.
            retry_on_grader_failure (bool, optional): Продолжать ли попытки, если не получилось оценить результаты SQL запроса.
                Defaults to True.
        """
        self.llm = llm or ChatOllama(base_url=get_config().OLLAMA_URL, model="qwen2.5:7b", temperature=0)
        self.llm_coder = llm_coder or ChatOllama(base_url=get_config().OLLAMA_URL, model="qwen2.5:7b", temperature=0)
        self.max_retries = max_retries
        self.db_engine = db_engine or create_engine(get_config().DB_DSN)
        self.retry_on_grader_failure = retry_on_grader_failure

        # prompts
        self.answer_prompt = ChatPromptTemplate.from_messages(
            [
                ("system", ANSWER_SYSTEM),
                ("user", "Вопрос пользователя: {question}"),
                MessagesPlaceholder(variable_name="data"),
            ]
        )
        self.generate_prompt = ChatPromptTemplate.from_messages(
            [
                ("system", GENERATE_SYSTEM),
                ("user", "Вопрос пользователя: {question}")
            ]
        )
        self.validate_prompt = ChatPromptTemplate.from_messages(
            [
                ("system", VALIDATE_SYSTEM),
                ("user", "SQL запрос: {query}")
            ]
        )
        
        # node specific llms
        self.llm_answerer = self.answer_prompt | self.llm.with_structured_output(AnswerResults)
        self.llm_generate = self.llm_coder.with_structured_output(GenerateQuery)

        # build graph
        workflow = StateGraph(GraphState)
        workflow.add_node("sql_generate", self._sql_generate)
        workflow.add_node("sql_validate", self._sql_validate)
        workflow.add_node("sql_execute", self._sql_execute)
        workflow.add_node("answer_results", self._answer_results)

        workflow.add_edge(START, "sql_generate")
        workflow.add_edge("sql_generate", "sql_validate")
        workflow.add_edge("sql_validate", "sql_execute")
        workflow.add_edge("sql_execute", "answer_results")
        workflow.add_conditional_edges("answer_results", self._retry_sql)

        self.agent = workflow.compile()

    def answer(self, question: str):
        """Получить ответ на вопрос

        Args:
            question (str): вопрос

        Returns:
            str: ответ
        """
        return self.agent.invoke({"question": question}).get("answer") or self.FAILED_ANSWER

    def _sql_generate(self, state: GraphState):
        question = state["question"]
        dialect_name = self.db_engine.dialect.name
        prompt = self.generate_prompt.invoke({
            "dialect": self.DIALECTS.get(dialect_name, dialect_name.title()),
            "question": question
        })
        response = cast(GenerateQuery, self.llm_generate.invoke(prompt))
        return state | {"current_sql": response.sql}

    def _sql_validate(self, state: GraphState):
        current_sql = state["current_sql"]
        dialect_name = self.db_engine.dialect.name
        prompt = self.validate_prompt.invoke({
            "dialect": self.DIALECTS.get(dialect_name, dialect_name.title()),
            "query": current_sql
        })
        response = cast(GenerateQuery, self.llm_generate.invoke(prompt))
        return state | {"current_sql": response.sql or current_sql}

    def _sql_execute(self, state: GraphState):
        current_sql = state["current_sql"]
        try:
            with self.db_engine.connect() as conn:
                resp = conn.execute(text(current_sql))
        except DatabaseError:
            state["attempts"] = state.get("attempts", 0) + 1
            return state
        csv_blob = io.StringIO(newline="")
        writer = csv.DictWriter(csv_blob, fieldnames=resp.keys())
        writer.writeheader()
        writer.writerows(row._asdict() for row in resp)
        db_results = state.get("db_results", [])
        db_results.append((current_sql, csv_blob.getvalue()))
        return state | {"db_results": db_results}

    def _answer_results(self, state: GraphState):
        if state.get("attempts", 0) >= 5:
            return state | {"answer": self.FAILED_ANSWER}

        if not state.get("db_results"):
            return state

        response = self.llm_answerer.invoke({
            "data": [
                (
                    "assistant",
                    f"SQL запрос: {sql}\n"
                    f"Результат: {res}\n"
                )
                for sql, res in state["db_results"]
            ],
            "question": state["question"]
        })
        return state | {"answer": cast(AnswerResults, response).answer}

    def _retry_sql(self, state: GraphState):
        if state.get("answer"):
            return END
        return "sql_generate"