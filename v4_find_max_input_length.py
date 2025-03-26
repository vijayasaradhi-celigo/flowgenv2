import json
from rich import print
from rich.progress import track
from transformers import T5Tokenizer

tokenizer = T5Tokenizer.from_pretrained('t5-small')
token_lengths = {}

def main():
    filename = 'ds_export_description_samples.jsonl'
    fp = open(filename, 'r')
    max_output_token_length = 0
    for line in track(fp, total=1203):
        obj = json.loads(line)
        description = obj['description']
        tokenized_length = get_t5_tokenized_length(description)
        token_lengths[tokenized_length] = token_lengths.get(tokenized_length, 0) + 1
        if obj['t5_tokens'] > max_output_token_length:
            max_output_token_length = obj['t5_tokens']

    sorted_token_lengths = sorted(token_lengths.items(), key=lambda x: x[0])
    total = sum([count for _, count in sorted_token_lengths])
    cumulative_total = 0
    for token_length, count in sorted_token_lengths:
        cumulative_total += count
        print(f'{token_length}: {count} percentage: {count/total*100:.2f}% cumulative percentage: {cumulative_total/total*100:.2f}%')
    print(f'Max output token length: {max_output_token_length}')


def get_t5_tokenized_length(text):
    tokenized_text = tokenizer(text)
    return len(tokenized_text['input_ids'])


if __name__ == '__main__':
    main()
