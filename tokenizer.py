"""
This file represents the tokenizer for the SQL statements, which takes in a string and breaks it
into parts
"""
import string


def collect_characters(query, allowed_characters):
    letters = []
    for letter in query:
        if letter not in allowed_characters:
            break
        letters.append(letter)
    return "".join(letters)


def remove_leading_whitespace(query, tokens):
    whitespace = collect_characters(query, string.whitespace)
    return query[len(whitespace):]


def remove_word(query, tokens):
    word = collect_characters(query,
                                   string.ascii_letters + "_.*" + string.digits)
    if word == "NULL":
        tokens.append(None)
    else:
        tokens.append(word)
    return query[len(word):]


def remove_text(query, tokens):
    assert query[0] == "'"
    tokens.append("'")
    query = query[1:]
    end_quote_index = query.find("'")

    # There is an escape character and there is one more '
    while query[end_quote_index + 1] == "'" and query[end_quote_index + 2:].find("'") != -1:
        query = query[:end_quote_index] + query[end_quote_index + 1:]
        end_quote_index += query[end_quote_index + 1:].find("'") + 1

    text = query[:end_quote_index]
    tokens.append(text)
    query = query[end_quote_index + 1:]
    tokens.append("'")
    return query


def remove_integer(query, tokens):
    int_str = collect_characters(query, string.digits)
    tokens.append(int_str)
    return query[len(int_str):]


def remove_number(query, tokens):
    query = remove_integer(query, tokens)
    if len(query) > 0 and query[0] == ".":
        whole_str = tokens.pop()
        query = query[1:]
        query = remove_integer(query, tokens)
        frac_string = tokens.pop()
        float_str = whole_str + "." + frac_string
        tokens.append(float(float_str))
    else:
        int_str = tokens.pop()
        tokens.append(int(int_str))
    return query


def tokenize(query):
    tokens = []
    while query:
        old_query = query

        if query[0:6] == "IS NOT":
            tokens.append(query[0:6])
            query = query[6:]
            continue

        if query[0] in string.whitespace:
            query = remove_leading_whitespace(query, tokens)
            continue

        if query[0:2] == "IS" or query[0:2] == "!=":
            tokens.append(query[0:2])
            query = query[2:]
            continue

        if query[0] in "(),;=><":
            tokens.append(query[0])
            query = query[1:]
            continue

        if query[0] in (string.ascii_letters + "_.*"):
            query = remove_word(query, tokens)
            continue

        if query[0] == "'":
            query = remove_text(query, tokens)
            continue

        if query[0] in string.digits:
            query = remove_number(query, tokens)
            continue

        if len(query) == len(old_query):
            raise AssertionError("Query didn't get shorter ")

    return tokens
