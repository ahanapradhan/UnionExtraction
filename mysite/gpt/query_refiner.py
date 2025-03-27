import os
import sys
from abc import abstractmethod

import tiktoken
from openai import OpenAI

from mysite.gpt.stack_benchmark import sQ9_text, stack_schema, sQ9_seed
from ..gpt.benchmark import Q1_text, Q1_seed, etpch_schema, general_guidelines, text_2_sql_question, \
    seed_query_question, Q1_seed_output, Q1_actual_output, Q3_text, Q3_seed, Q3_seed_output, Q3_actual_output, Q4_text, \
    Q4_seed_output, Q4_actual_output, Q4_seed, Q4_feedback1, Q3_feedback1, Q5_text, Q5_seed, Q5_seed_output, \
    Q5_actual_output, Q6_text, Q6_seed_output, Q6_actual_output, Q6_seed, Q14_text, Q14_seed, Q14_seed_output, \
    Q14_actual_output, Q14_feedback1, refinement_show, Q7_text, Q7_seed, Q7_seed_output, Q7_actual_output, Q7_feedback1, \
    Q21_text, Q21_seed, Q21_seed_output, Q21_actual_output, Q21_feedback1, Q8_text, Q8_seed, \
    Q8_seed_output, Q8_actual_output, Q8_feedback1, Q23_text, Q23_seed, Q24_text, Q24_seed, Q24_seed_output, \
    Q24_actual_output, Q18_text, Q18_seed, Q18_seed_output, Q18_actual_output, \
    Q18_feedback1, Q9_actual_output, Q9_seed_output, Q9_seed, Q9_text, Q10_text, Q10_seed, Q10_seed_output, \
    Q10_actual_output, Q11_text, Q11_seed, Q11_seed_output, Q11_actual_output, Q11_feedback1, Q12_text, Q12_seed, \
    Q12_seed_output, Q12_actual_output, Q12_feedback1, Q15_text, Q15_seed, Q15_seed_output, Q15_actual_output, \
    Q15_feedback1, Q20_text, Q20_seed, Q20_seed_output, Q20_actual_output, Q2_text, Q2_seed, \
    Q2_seed_output, Q2_actual_output, Q16_text, Q16_seed, Q22_text, Q22_seed, Q22_seed_output, \
    Q22_actual_output, Q22_feedback1, Q13_text, Q13_seed, Q13_seed_output, Q13_actual_output, Q13_feedback1, Q19_text, \
    Q19_seed, Q19_seed_output, Q17_text, Q17_seed, Q17_seed_output, Q17_actual_output, Q13_feedback2, Q4_feedback2, \
    Q16_feedback2, Q18_feedback2, Q20_feedback1, Q20_feedback2, Q13_feedback11, Q13_feedback12

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

    def doJob_write(self, text, key, append=False):
        print(text)
        working_dir = self.working_dir_path + self.output_dirname
        if not os.path.exists(working_dir):
            os.makedirs(working_dir)

        filename = f"{working_dir}{self.name}_{key}_{self.output_filename}"

        orig_out = sys.stdout
        mode = 'a' if append else 'w'
        f = open(filename, mode)
        sys.stdout = f
        reply = self.doJob(text)
        sys.stdout = orig_out
        f.close()
        mode_name = 'appended' if mode == 'a' else 'written'
        print(f"SQL {mode_name} into {filename}")
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
                  "Q2": [Q2_text, Q2_seed, Q2_seed_output, Q2_actual_output],
                  "Q3": [Q3_text, Q3_seed, Q3_seed_output, Q3_actual_output, [Q3_feedback1]],
                  "Q4": [Q4_text, Q4_seed, Q4_seed_output, Q4_actual_output, [Q4_feedback1, Q4_feedback2]],
                  "Q5": [Q5_text, Q5_seed, Q5_seed_output, Q5_actual_output],
                  "Q6": [Q6_text, Q6_seed, Q6_seed_output, Q6_actual_output],
                  "Q7": [Q7_text, Q7_seed, Q7_seed_output, Q7_actual_output, [Q7_feedback1]],
                  "Q8": [Q8_text, Q8_seed, Q8_seed_output, Q8_actual_output, [Q8_feedback1]],
                  "Q9": [Q9_text, Q9_seed, Q9_seed_output, Q9_actual_output],
                  "Q10": [Q10_text, Q10_seed, Q10_seed_output, Q10_actual_output],
                  "Q11": [Q11_text, Q11_seed, Q11_seed_output, Q11_actual_output, [Q11_feedback1]],
                  "Q12": [Q12_text, Q12_seed, Q12_seed_output, Q12_actual_output, [Q12_feedback1]],
                  "Q13": [Q13_text, Q13_seed, Q13_seed_output, Q13_actual_output, [Q13_feedback11, Q13_feedback12]],
                  "Q14": [Q14_text, Q14_seed, Q14_seed_output, Q14_actual_output, [Q14_feedback1]],
                  "Q15": [Q15_text, Q15_seed, Q15_seed_output, Q15_actual_output, [Q15_feedback1]],
                  "Q16": [Q16_text, Q16_seed, "", "", [Q16_feedback2]],
                  "Q17": [Q17_text, Q17_seed, Q17_seed_output, Q17_actual_output],
                  "Q18": [Q18_text, Q18_seed, Q18_seed_output, Q18_actual_output, [Q18_feedback1, Q18_feedback2]],
                  "Q19": [Q19_text, Q19_seed, Q19_seed_output, ""],
                  "Q20": [Q20_text, Q20_seed, Q20_seed_output, Q20_actual_output],
                  "Q21": [Q21_text, Q21_seed, Q21_seed_output, Q21_actual_output, [Q21_feedback1]],
                  "Q22": [Q22_text, Q22_seed, Q22_seed_output, Q22_actual_output, [Q22_feedback1]],
                  "Q23": [Q23_text, Q23_seed],
                  "Q24": [Q24_text, Q24_seed, Q24_seed_output, Q24_actual_output],
                  "sQ9": [sQ9_text, sQ9_seed]
                  }


def get_feedback_prompts(key):
    try:
        last = benchmark_dict[key][-1]
        if isinstance(last, list):
            return benchmark_dict[key][-1]
    except IndexError:
        pass
    return ""


def get_synonymous_schema(key):
    try:
        return benchmark_dict[key][4]
    except IndexError:
        return ""


def get_text(key):
    try:
        return benchmark_dict[key][0]
    except IndexError:
        return ""


def get_seed(key):
    try:
        return benchmark_dict[key][1]
    except IndexError:
        return ""


def get_seed_output(key):
    try:
        return benchmark_dict[key][2]
    except IndexError:
        return ""


def get_actual_output(key):
    try:
        return benchmark_dict[key][3]
    except IndexError:
        return ""


def do_feedback_refinement(no_show=False):
    global prompt
    needed_feedback = len(get_feedback_prompts(query_key))
    if needed_feedback:
        feedbacks = get_feedback_prompts(query_key)
        for i, feedback in enumerate(feedbacks):
            print(f"Trying out feedback {i + 1}")
            if not no_show:
                prompt = f"{prompt}\n" \
                         f"{refinement_show}\n{output1}" \
                         f"\n{feedback}"
            else:
                prompt = f"{prompt}\n" \
                         f"\n{feedback}"
            output2 = refiner.doJob_write(prompt, query_key, True)
            print(output2)


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Specify LLM name!")
        exit()
    if len(sys.argv) < 3:
        print("Specify Query key name!")
        exit()
    model_name = sys.argv[1]
    query_key = sys.argv[2]
    if not len(get_text(query_key)):
        print("No input prompt!")
        exit()

    refiner = create_query_refiner(model_name)
    schema = stack_schema  # etpch_schema

    unique_prompt = f"{get_text(query_key)}\n" \
                    f"{seed_query_question}\n{get_seed(query_key)}" \
                    f"\n{get_seed_output(query_key)}\n{get_actual_output(query_key)}"
    # print(unique_prompt)

    prompt = f"{text_2_sql_question}\n{unique_prompt}" \
             f"\n{schema}\n{general_guidelines}"

    """
    # print(unique_prompt)
    prompt = f"{text_2_sql_question}\n{unique_prompt}" \
             f"\n{schema}"
    """
    output1 = refiner.doJob_write(prompt, query_key)
    print(output1)
    do_feedback_refinement(no_show=True)
