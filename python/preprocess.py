#!/usr/bin/env python3

import string
import re
import pandas as pd


class Preprocessing:
    def transform_remove_outliers(Body):
        return ' '.join([word for word in Body.split() if len(word) > 2 and len(word) < 25])

    def transform_remove_URL(Body):
        url = re.compile(r'https?://\S+|www\.\S+')
        return url.sub(r'', Body)

    def transform_remove_html(Body):
        html = re.compile(r'<.*?>|&([a-z0-9]+|#[0-9]{1,6}|#x[0-9a-f]{1,6});')
        return re.sub(html, '', Body)

    def transform_remove_usernames(Body):
        uh = re.compile(r'([@][A-Za-z0-9_]+)|(\w+:\/\/\S+)')
        return uh.sub(r'', Body)

    def transform_remove_hashtags(Body):
        return re.sub(r'#\w+', ' ', Body)

    def transform_remove_emoji(Body):
        emoji_pattern = re.compile("["
                                   u"\U0001F600-\U0001F64F"  # emoticons
                                   u"\U0001F300-\U0001F5FF"  # symbols & pictographs
                                   u"\U0001F680-\U0001F6FF"  # transport & map symbols
                                   u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
                                   u"\U00002500-\U00002BEF"  # chinese char
                                   u"\U00002702-\U000027B0"
                                   u"\U00002702-\U000027B0"
                                   u"\U000024C2-\U0001F251"
                                   u"\U0001f926-\U0001f937"
                                   u"\U00010000-\U0010ffff"
                                   u"\u2640-\u2642"
                                   u"\u2600-\u2B55"
                                   u"\u200d"
                                   u"\u23cf"
                                   u"\u23e9"
                                   u"\u231a"
                                   u"\ufe0f"  # dingbats
                                   u"\u3030"
                                   "]+", re.UNICODE)
        return emoji_pattern.sub(r' ', Body)

    def transform_remove_digits(Body):
        return re.sub(r'\d+', ' ', Body)

    def transform_lowercase(Body):
        return Body.lower()

    def transform_remove_non_alphanumeric(Body):
        # Remove non-alphanumeric characters
        processed_sentence = re.sub(r'[^a-zA-Z0-9\s]', '', Body)
        return processed_sentence

    def transform_fix_i(Body):
        fix = re.compile(r'i̇')
        return fix.sub(r'i', Body)

    def transform_fix_whitespace(Body):
        return ' '.join(Body.split())

    def transform_remove_punct(Body):
        table = str.maketrans('', '', string.punctuation)
        return Body.translate(table)

    def transform_all(Body):
        transforms = [
            Preprocessing.transform_remove_URL,
            Preprocessing.transform_remove_html,
            Preprocessing.transform_remove_usernames,
            Preprocessing.transform_remove_hashtags,
            Preprocessing.transform_remove_emoji,
            Preprocessing.transform_remove_digits,
            Preprocessing.transform_lowercase,
            Preprocessing.transform_remove_non_alphanumeric,
            Preprocessing.transform_fix_i,
            Preprocessing.transform_fix_whitespace,
            Preprocessing.transform_remove_punct,
            Preprocessing.transform_remove_outliers,
        ]
        for transform in transforms:
            Body = transform(Body)

        return Body

    def apply_transforms(df):
        df = df.apply(lambda x: Preprocessing.transform_all(x))
        return df


if __name__ == "__main__":
    # Read input csv files
    answers = pd.read_csv("jobs/data/Answers.csv", engine='c', encoding="ISO-8859-1",
                          dtype={'Id': 'Int64', 'OwnerUserId': 'Int64',
                                 'CreationDate': str,
                                 'ParentId': 'Int64', 'Score': 'Int64',
                                 'Body': str}
                          )
    questions = pd.read_csv("jobs/data/Questions.csv", engine='c', encoding="ISO-8859-1",
                            dtype={'Id': 'Int64', 'OwnerUserId': 'Int64',
                                   'CreationDate': str, 'ClosedDate':
                                   str, 'Score': 'Int64', 'Title': str,
                                   'Body': str}
                            )
    tags = pd.read_csv("jobs/data/Tags.csv", engine='c', encoding="ISO-8859-1",
                       dtype={'Id': 'Int64', 'Tag': str})

    # Remove not needed rows
    questions.drop(columns='ClosedDate', inplace=True)
    questions.dropna(inplace=True)
    answers.dropna(inplace=True)

    # Preprocess csv files, make all entries single line
    questions["Body"] = Preprocessing.apply_transforms(questions["Body"])
    questions["Title"] = Preprocessing.apply_transforms(questions["Title"])
    answers["Body"] = Preprocessing.apply_transforms(answers["Body"])

    # Group tags with whitespace seperator, add as column to "questions"
    tags['Tag'] = tags['Tag'].astype(str)
    grouped_tags = tags.groupby("Id")['Tag'].apply(lambda tags: ' '.join(tags))
    grouped_tags.reset_index()
    grouped_tags_final = pd.DataFrame({'Id': grouped_tags.index, 'Tags': grouped_tags.values})
    questions = questions.merge(grouped_tags_final, on='Id')

    # Remove rows with too less data
    word_count = questions['Body'].apply(lambda x: len(str(x).split(" ")))
    questions = questions[word_count >= 3]
    questions = questions[questions['Score'] > 0]

    word_count = answers['Body'].apply(lambda x: len(str(x).split(" ")))  # Kelime sayısını hesaplayın
    answers = answers[word_count >= 3]

    # Save the column data to the Body file
    questions.to_csv("jobs/data/QuestionsPre.csv", header=False, index=False, na_rep='NA')
    answers.to_csv("jobs/data/AnswersPre.csv", header=False, index=False, na_rep='NA', )
