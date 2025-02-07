import os
import sys
from abc import abstractmethod

import tiktoken
from openai import OpenAI

from mysite.gpt.Q13_benchmark import Q13_text, Q13_seed, Q13_seed_output, Q13_actual_output, etpch_schema_Q13, \
    Q13_feedback1, Q13_feedback2, Q13_feedback4_sample_data
from mysite.gpt.benchmark import Q1_text, Q1_seed, etpch_schema, general_guidelines, text_2_sql_question, \
    seed_query_question, Q1_seed_output, Q1_actual_output, Q3_text, Q3_seed, Q3_seed_output, Q3_actual_output, Q4_text, \
    Q4_seed_output, Q4_actual_output, Q4_seed, Q4_feedback1, Q3_feedback1, Q5_text, Q5_seed, Q5_seed_output, \
    Q5_actual_output

# gets API Key from environment variable OPENAI_API_KEY
client = OpenAI()


def create_query_refiner(gpt_model):
    if gpt_model == "o3":
        return GptO3miniRefiner()
    elif gpt_model == "4o":
        return Gpt4oRefiner()
    else:
        raise ValueError("Model not supported!")


class Refiner:
    def __init__(self, name):
        self.name = name
        self.working_dir_path = "mysite/gpt/"
        self.output_dirname = "output/"
        self.output_filename = "chatgpt.sql"

    @abstractmethod
    def count_tokens(self, text):
        pass

    @abstractmethod
    def doJob(self, text):
        pass

    def doJob_write(self, text, key):
        working_dir = self.working_dir_path + self.output_dirname
        if not os.path.exists(working_dir):
            os.makedirs(working_dir)

        filename = f"{working_dir}{self.name}_{key}_{self.output_filename}"

        orig_out = sys.stdout
        f = open(filename, 'w')
        sys.stdout = f
        reply = self.doJob(text)
        sys.stdout = orig_out
        f.close()
        print(f"SQL saved into {filename}")
        return reply


class GptO3miniRefiner(Refiner):
    def __init__(self):
        super().__init__("o3-mini")

    def count_tokens(self, text):
        raise NotImplementedError

    def doJob(self, text):
        response = client.chat.completions.create(
            model=self.name,
            messages=[
                {
                    "role": "user",
                    "content": f"{text}",
                },
            ]
        )
        reply = response.choices[0].message.content
        print(reply)
        return reply


class Gpt4oRefiner(Refiner):

    def __init__(self):
        super().__init__("gpt-4o")

    def count_tokens(self, text):
        encoding = tiktoken.encoding_for_model(self.name)
        tokens = encoding.encode(text)
        return len(tokens)

    def doJob(self, text):
        response = client.chat.completions.create(
            model=self.name,
            messages=[
                {
                    "role": "user",
                    "content": f"{text}",
                },
            ], temperature=0, stream=False
        )
        reply = response.choices[0].message.content
        print(reply)
        c_token = self.count_tokens(text)
        print(f"\n-- Prompt Token count = {c_token}\n")
        return reply


benchmark_dict = {"Q1": [Q1_text, Q1_seed, Q1_seed_output, Q1_actual_output],
                  "Q3": [Q3_text, Q3_seed, Q3_seed_output, Q3_actual_output, [Q3_feedback1]],
                  "Q4": [Q4_text, Q4_seed, Q4_seed_output, Q4_actual_output, [Q4_feedback1]],
                  "Q5": [Q5_text, Q5_seed, Q5_seed_output, Q5_actual_output],
                  "Q13": [Q13_text, Q13_seed, Q13_seed_output, Q13_actual_output,
                          etpch_schema_Q13, [Q13_feedback1, Q13_feedback2, Q13_feedback4_sample_data]]}


def get_feedback_prompts(key):
    last = benchmark_dict[key][-1]
    if isinstance(last, list):
        return benchmark_dict[key][-1]
    return ""


def get_synonymous_schema(key):
    if len(benchmark_dict[key]) > 4:
        return benchmark_dict[key][4]
    return ""


def get_text(key):
    return benchmark_dict[key][0]


def get_seed(key):
    if len(benchmark_dict[key]) < 2:
        return ""
    return benchmark_dict[key][1]


def get_seed_output(key):
    if len(benchmark_dict[key]) < 3:
        return ""
    return benchmark_dict[key][2]


def get_actual_output(key):
    if len(benchmark_dict[key]) < 4:
        return ""
    return benchmark_dict[key][3]


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Specify LLM name!")
        exit()
    if len(sys.argv) < 3:
        print("Specify Query key name!")
        exit()
    model_name = sys.argv[1]
    query_key = sys.argv[2]
    refiner = create_query_refiner(model_name)
    schema = get_synonymous_schema(query_key)
    if not len(schema):
        schema = etpch_schema
    prompt = f"{text_2_sql_question}\n{get_text(query_key)}\n" \
             f"{seed_query_question}\n{get_seed(query_key)}" \
             f"\n{get_seed_output(query_key)}\n{get_actual_output(query_key)}" \
             f"\n{schema}\n{general_guidelines}"
    output1 = refiner.doJob_write(prompt, query_key)
    print(output1)
    needed_feedback = len(get_feedback_prompts(query_key))
    if needed_feedback:
        feedbacks = get_feedback_prompts(query_key)
        for i, feedback in enumerate(feedbacks):
            print(f"Trying out feedback {i + 1}")
            prompt = f"{text_2_sql_question}\n{get_text(query_key)}\n" \
                     f"{seed_query_question}\n{output1}" \
                     f"\n{feedback}" \
                     f"\n{schema}\n{general_guidelines}"
            output2 = refiner.doJob_write(prompt, query_key)
            print(output2)
