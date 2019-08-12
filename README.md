# Data service assembler

Service assembler that joins data by key from multiple topics based on Kafka streams Ktables joins
Includes a Message producer for multiple topics
Includes a Message consumer from output topic

# Input

Topics:
    input-nlu-service-1
    input-nlu-service-2

# Output

Topic:
    output-data-assembler

## Data

Events:
    "7dc53df5-703e-49b3-8670-b1c468f47f1f"|{"uuid": "7dc53df5-703e-49b3-8670-b1c468f47f1f", "message" : "Test message A"}
    "7dc53df5-703e-49b3-8670-b1c468f47f1f"|{"uuid": "7dc53df5-703e-49b3-8670-b1c468f47f1f", "message" : "Test message B"}