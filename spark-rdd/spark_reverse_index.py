from pyspark import SparkContext, SparkConf
import json
import re
import sys

_, source, output = sys.argv

conf = SparkConf().setAppName("spark-reverse-index")
sc = SparkContext(conf=conf)

# Read JSON lines
lines = sc.textFile(source)


def parse_article(line: str):
    try:
        doc = json.loads(line)
        doc_id = str(doc.get("id", ""))
        # Get abstract or text, concatenate them
        text = doc.get("abstract", "") + " " + doc.get("text", "")
        return [(doc_id, text)]
    except:
        return []

def tokenize(text: str): # tokenize text into words, lowercased, alphanumeric only
    text = text.lower()
    words = re.findall(r"\b[a-z0-9]+\b", text)
    return words

def emit_word_doc_pairs(doc_id_text): #emit (word, docId) pairs for each word in the text
    doc_id, text = doc_id_text
    words = tokenize(text)
    return [(word, doc_id) for word in words] # Return (word, docId) for each word

# Create inverted index
inverted_index = (
    lines
    .flatMap(parse_article)  # Parse JSON and get (docId, text)
    .flatMap(emit_word_doc_pairs)  # Emit (word, docId) pairs
    .distinct()  # Remove duplicate (word, docId) pairs
    .groupByKey()  # Group by word to get (word, [docIds])
    .map(lambda x: (x[0], sorted(set(x[1]))))  # Sort and dedupe docIds
)

# Format output as: WORD DOCID1 DOCID2 DOCID3 ...
result = inverted_index.map(lambda pair: f"{pair[0]} {' '.join(pair[1])}")

# Save to file
result.coalesce(1).saveAsTextFile(output)

print("reverse index created and saved to", output)

sc.stop()
