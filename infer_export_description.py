import pandas as pd
from rich import print
from collections import Counter
from rich.progress import track
import sys
resources_df = pd.read_csv('all_resources.csv')
connectors_df = pd.read_csv('all_connectors.csv')

def main():
    final_rows = []
    filename = 'exports_with_assistants_resources.csv'
    df = pd.read_csv(filename)
    #print(df)
    #print(df.shape)

    # Iterate the rows of the dataframe
    cx = 0
    num_success = 0
    for index, row in track(df.iterrows(), total=df.shape[0]):
        cx += 1
        if cx >=1000: 
        #    print("Num success: ", num_success)
            break
        # Convert row to an object
        row = dict(row)
        #print(row)
        assistant = row.get('assistant')
        if assistant is None:
            continue
        try:
            ret_val = infer_resource(row)
        except Exception as e:
            raise
            continue
        if ret_val is None:
            continue
        print(ret_val)
        num_success += 1
        inferred_resource = ret_val['inferred_resource']
        inferred_description = ret_val['inferred_description']
        #http_response_resourcepath = str(row['http_response_resourcepath']).strip('"')
        name = row['name'].strip('"')
        description = str(row['description']).strip('"')
        assistant = row['assistant'].strip('"')
        name = name.strip('"')

        final_row = {
                'id': row['id'].strip('"'),
                'assistant': assistant,
                #'http_response_resourcepath': http_response_resourcepath,
                'name': name,
                'description': description,
                'http_relativeuri': row['http_relativeuri'],
                'inferred_resource': inferred_resource,
                'inferred_description': inferred_description,
                'inferred_reccord_type': ret_val['inferred_record_type']
                }
        print(final_row)
        final_rows.append(final_row)
    print("Number of successful inferences: ", num_success)

    # Create a new dataframe
    df = pd.DataFrame(final_rows)
    print(df)

    # Sort the df by assistant and http_response_resourcepath
    df = df.sort_values(by=['assistant', 'http_relativeuri'])
    df.to_csv('inferred_exports_new.csv', index=False)

def infer_resource(row):
    assistant = row['assistant'].strip('"').lower()
    method = row['http_method'].strip('"')
    relative_uri = str(row['http_relativeuri']).strip('"')
    #print("Entered infer_resource with assistant: ", assistant)
    #print("method=", method, "relative_uri=", relative_uri)
    #print("Data: ", row)
    # Filter the connectors_df by assistant and method
    mini_connector_df = connectors_df[(connectors_df['connector'] == assistant) &
                                   (connectors_df['method'] == method)]
    #print(mini_connector_df)
    relative_uris = mini_connector_df['relative_uri'].tolist()
    best_match = find_best_uri_template(relative_uri, relative_uris)
    if best_match is None:
        print("None returned for", relative_uri)
        print("Relative uris: ", relative_uris)
    #print("Best match for relative_uri: ", best_match)
    if best_match is None: return None
    best_connector_row = mini_connector_df[mini_connector_df['relative_uri'] ==
                                           best_match]

    #print("Shape of best_connector_row is ", best_connector_row.shape)
    row_objs = best_connector_row.to_dict('records')
    if len(row_objs) > 0:
        row_obj = row_objs[0]
    #print("Best connector row: ", row_obj)
    obj={}
    obj['inferred_resource'] = row_obj['relative_uri']
    obj['inferred_description'] =row_obj['name'] 
    obj['inferred_record_type'] = infer_record_type(assistant, row_obj['name'])
    return obj

def infer_record_type(assistant, description):
    print("Infer record type with assistant: ", assistant, "description: ", description)
    # Filter the resources_df by assistant and description
    mini_resources_df = resources_df[resources_df['connector'] == assistant]
    #print(mini_resources_df)
    resources = mini_resources_df['resource'].tolist()
    similarities = [(resource, count_ngrams(description, resource)) for resource in resources]
    #print("Similarities: ", similarities)
    # Sort the similarities based on the count of ngrams
    similarities = sorted(similarities, key=lambda x: x[1], reverse=True)
    #print("Sorted similarities: ", similarities)
    if len(similarities) > 0:
        return similarities[0][0]

def count_characters(w1, w2):
    return sum(1 for char in w2 if char in w1)
def count_ngrams(w1, w2):
    """
    Given two words w1 and w2, this function returns the count of all character ngrams 
    (i.e., all contiguous substrings) in w2 that are also present in w1.
    
    For example, for w1 = "apple" the ngrams include:
      'a', 'p', 'p', 'l', 'e', 'ap', 'pp', 'pl', 'le', 'app', 'ppl', 'ple', 
      'appl', 'pple', and 'apple'
      
    Parameters:
        w1 (str): The first word.
        w2 (str): The second word.
        
    Returns:
        int: The number of ngrams in w2 that appear in w1.
    """
    # Generate all ngrams for w1 and store in a set for fast look-up.
    ngrams_w1 = set()
    for i in range(len(w1)):
        for j in range(i + 1, len(w1) + 1):
            ngrams_w1.add(w1[i:j])
    
    # Count ngrams in w2 that are also in w1.
    count = 0
    for i in range(len(w2)):
        for j in range(i + 1, len(w2) + 1):
            if w2[i:j] in ngrams_w1:
                count += 1
    return count


def jaccard_similarity(s1: str, s2: str) -> float:
    # Convert strings to lowercase and split them into words
    words1 = set(s1.lower().split())
    words2 = set(s2.lower().split())
    return len(words1.intersection(words2)) / len(words1.union(words2))
    
    # Compute the intersection and union of the two sets
    intersection = words1.intersection(words2)
    union = words1.union(words2)
    
    # Return the Jaccard similarity; handle the case when union is empty
    return len(intersection) / len(union) if union else 0.0

words_to_ignore = ['{', '}', 'export.http.paging.page', '.json', '.xml', '/v1/',
                   '/v2/', '/v3/', '/v4/', '/v5/', '/v6/', '/v7/', '/v8/',
                   'json','{{{export.http.paging.skip}}}']

def is_plural(word):
    if word[-1] == 's':
        return True
    return False

def longest_common_substring(s1, s2):
    """
    Finds the longest common substring between s1 and s2.
    
    Parameters:
        s1 (str): First input string.
        s2 (str): Second input string.
    
    Returns:
        str: The longest substring that appears in both s1 and s2.
             If there is no common substring, returns an empty string.
    """
    for word in words_to_ignore:
        s1 = s1.replace(word, '')
        s2 = s2.replace(word, '')
    
    m, n = len(s1), len(s2)
    # Create a 2D DP table with (m+1) x (n+1) dimensions, initialized with zeros.
    dp = [[0] * (n + 1) for _ in range(m + 1)]
    longest = 0  # Length of longest common substring found
    end_index = 0  # Ending index of longest common substring in s1

    # Build the table in bottom-up fashion.
    for i in range(1, m + 1):
        for j in range(1, n + 1):
            if s1[i - 1] == s2[j - 1]:
                dp[i][j] = dp[i - 1][j - 1] + 1
                if dp[i][j] > longest:
                    longest = dp[i][j]
                    end_index = i
            else:
                dp[i][j] = 0  # Reset since substring must be contiguous

    # Extract the longest common substring from s1 using the end index and length.
    return s1[end_index - longest:end_index]

import re
from typing import List, Optional, Tuple

def _convert_without_query(template: str, dummy: str = "IMAGINED") -> Tuple[str, int, str]:
    """
    Converts a template segment (without query splitting) into a regex pattern.
    It first processes conditional blocks so that the block’s content is optional,
    then replaces tokens (like :param, {{var}}, {{{var}}}) with regex groups.
    
    Returns a tuple of (regex_pattern, specificity, imagined_template) where:
      - regex_pattern is a complete regex anchored with ^ and $.
      - specificity is the total length of literal text (used to choose the most-specific template).
      - imagined_template is the template with all tokens replaced by the dummy value.
    """
    def process_block(text: str) -> str:
        block_re = re.compile(r'\{\{#.*?\}\}(.*?)\{\{\/.*?\}\}', re.DOTALL)
        def repl(m):
            inner = m.group(1)
            inner_regex, inner_spec, _ = _convert_without_query(inner, dummy)
            # Remove anchors (^ and $) from the inner regex.
            inner_regex = inner_regex[1:-1]
            return f"(?:{inner_regex})?"
        return block_re.sub(repl, text)
    
    processed_template = process_block(template)
    
    token_re = re.compile(r'(:\w+|\{\{\{[^}]+\}\}\}|\{\{[^}]+\}\})')
    
    regex_parts = []
    specificity = 0
    last_index = 0
    
    for match in token_re.finditer(processed_template):
        literal = processed_template[last_index:match.start()]
        if literal:
            regex_parts.append(re.escape(literal))
            specificity += len(literal)
        regex_parts.append("([^/]+)")
        last_index = match.end()
    
    trailing = processed_template[last_index:]
    if trailing:
        regex_parts.append(re.escape(trailing))
        specificity += len(trailing)
    
    regex_pattern = "^" + "".join(regex_parts) + "$"
    imagined_template = token_re.sub(dummy, processed_template)
    return regex_pattern, specificity, imagined_template

def convert_template_to_regex(template: str, dummy: str = "IMAGINED") -> Tuple[str, int, str]:
    """
    Converts a URI template into a regex pattern.
    
    If the template includes a "?" (i.e. query parameters), it is split into a base part and a query part.
    The query part is converted separately and then wrapped in an optional non-capturing group.
    
    If no query part is defined in the template, this version allows an optional query string,
    so that a URL like "/orders.json?updated_at_min=..." can still match the template "/orders.json".
    """
    if "?" in template:
        base, query = template.split("?", 1)
        base_regex, base_spec, base_imagined = _convert_without_query(base, dummy)
        query_regex, query_spec, query_imagined = _convert_without_query(query, dummy)
        
        base_inner = base_regex[1:-1]
        query_inner = query_regex[1:-1]
        
        full_regex = "^" + base_inner + "(?:\\?" + query_inner + ")?$"
        total_spec = base_spec + query_spec
        imagined_template = base_imagined + "?" + query_imagined
        return full_regex, total_spec, imagined_template
    else:
        # For templates with no query parameters, allow an optional query string.
        regex_pattern, specificity, imagined_template = _convert_without_query(template, dummy)
        # Remove the trailing "$" and add an optional query part.
        regex_pattern = regex_pattern[:-1] + r"(?:\?.*)?$"
        return regex_pattern, specificity, imagined_template

def find_best_uri_template(url: str, templates: List[str], dummy: str = "IMAGINED") -> Optional[str]:
    """
    Given a URL and a list of URI templates, returns the template that best matches the URL.
    
    For each template the function:
      1. Converts the template into a regex pattern (processing tokens, query strings, and conditional blocks).
      2. Tests the URL against that regex.
    
    If more than one template matches, the one with the highest specificity (i.e. most fixed literal content) is chosen.
    """
    best_template = None
    best_specificity = -1
    
    for template in templates:
        regex_pattern, specificity, imagined_template = convert_template_to_regex(template, dummy)
        if re.match(regex_pattern, url):
            # Debug: print(f"Template: {template}\n  Imagined: {imagined_template}\n  Regex: {regex_pattern}\n  Specificity: {specificity}\n")
            if specificity > best_specificity:
                best_specificity = specificity
                best_template = template
                
    return best_template

if __name__ == "__main__":
    main()

