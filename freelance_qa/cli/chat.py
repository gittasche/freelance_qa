import click

from freelance_qa.llm import FreelanceQA


@click.command
def chat():
    qa = FreelanceQA()
    print("Введите exit для завершения")
    while True:
        print("Вопрос: ", end="")
        question = input().strip()
        if question.lower() == "exit":
            print("Выход")
            break
        print(qa.answer(question))