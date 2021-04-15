## Orders Database

### Orders Database Schema
Orders database is a NoSQL key-value database hosted on AWS DynamoDB, developed by AWS Python SDK.

### Usage
The there several operations supported.
1. Create a table
2. Load data
3. CRUD operations for item
4. Query 

### Deployment
DynamoDB has a local version which can be run on a user's desktop. It simplifies development and saves cost. To deploy local version, download DynamoDB local and AWS CLI.
1. Change directory to which contains `DynamoDBLocal.jar`
2. Start a local DynamoDB: `java -D"java.library.path=./DynamoDBLocal_lib" -jar DynamoDBLocal.jar`
3. (Optional) Check existing table(s): `aws dynamodb list-tables --endpoint-url http://localhost:8000`
4. Run scripts
