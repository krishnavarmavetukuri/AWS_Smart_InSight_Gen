import boto3
import json
from collections import defaultdict
from datetime import datetime

# Using Amazon DynamoDB to store both detailed analysis and sentiment-level summaries
dynamodb = boto3.resource('dynamodb')

# Stores raw review analysis from Comprehend
detailed_table = dynamodb.Table('CustomerReviewsAnalysis')

# Stores generated sentiment-level summary
summary_table = dynamodb.Table('SentimentReviewSummaries')


def lambda_handler(event, context):
    # Full-table scan to retrieve all reviews - prepares for grouping by sentiment
    reviews = get_all_reviews()

    if not reviews:
        return {
            'statusCode': 200,
            'body': json.dumps('No reviews found for summarization.')
        }

    # Grouping by sentiment (POSITIVE, NEGATIVE, NEUTRAL, MIXED) - Insight generation by sentiment
    sentiment_groups = defaultdict(list)

    for review in reviews:
        sentiment = review.get('Sentiment', 'NEUTRAL')
        sentiment_groups[sentiment].append(review)

    # For each sentiment group, generate a summary based on key phrases - Summarization logic
    for sentiment, group_reviews in sentiment_groups.items():
        key_phrase_counter = defaultdict(int)
        
        # Aggregating key phrases from all reviews in this sentiment group
        for review in group_reviews:
            key_phrases = review.get('KeyPhrases', [])
            for phrase in key_phrases:
                key_phrase_counter[phrase] += 1

        # Selecting top 10 most common key phrases - relevant insight extraction
        top_phrases = sorted(key_phrase_counter.items(), key=lambda x: x[1], reverse=True)[:10]
        summary_text = "Top phrases: " + ", ".join([phrase for phrase, _ in top_phrases])
        total_reviews = len(group_reviews)

        # Storing sentiment-wise summary into DynamoDB - Final stage: structured insight storage
        summary_table.put_item(
            Item={
                'Sentiment': sentiment,
                'SummaryDate': datetime.now().strftime('%Y-%m-%d'),
                'SummaryText': summary_text,
                'TotalReviews': total_reviews
            }
        )

    return {
        'statusCode': 200,
        'body': json.dumps('Sentiment summaries generated and saved.')
    }


def get_all_reviews():
    # Scans the detailed analysis table to retrieve all review entries
    response = detailed_table.scan()
    items = response.get('Items', [])
    
    # Handling pagination for larger datasets - scalability support
    while 'LastEvaluatedKey' in response:
        response = detailed_table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
        items.extend(response.get('Items', []))

    return items
