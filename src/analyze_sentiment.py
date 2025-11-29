import pandas as pd
from textblob import TextBlob

def get_sentiment(text):
    if not isinstance(text, str):
        return "neutral", 0

    score = TextBlob(text).sentiment.polarity
    if score > 0.1:
        return "positive", score
    elif score < -0.1:
        return "negative", score
    else:
        return "neutral", score

def apply_sentiment(df):
    df[['sentiment_label','sentiment_score']] = df['review'].apply(lambda x: pd.Series(get_sentiment(x)))
    return df
