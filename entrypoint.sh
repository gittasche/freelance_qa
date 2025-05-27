#!/bin/bash

/bin/ollama serve &
pid=$!

sleep 5

ollama pull qwen2.5:7b

wait $pid
