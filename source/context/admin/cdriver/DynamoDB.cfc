component extends="Cache" {
	variables.fields = [
		field(displayName = "Table Name",
			name = "tableName",
			defaultValue = "",
			required = true,
			description = "The name of the DynamoDB table to use for caching. The table will be created automatically if it doesn't exist. The table uses a simple key-value structure with 'id' as the partition key.",
			type = "text"
		)

		,group("Authentication","AWS credentials for accessing DynamoDB. Not required when running on EC2/ECS with IAM roles, but required for local development or external access.")
		,field(displayName = "AccessKeyId",
			name = "accesskeyid",
			defaultValue = "",
			required = false,
			description = "AWS Access Key ID for authentication. Leave empty when running on EC2/ECS with IAM roles. For DynamoDB Local testing, you can use any dummy value like 'dummy'.",
			type = "text"
		)
		,field(displayName = "SecretKey",
			name = "secretkey",
			defaultValue = "",
			required = false,
			description = "AWS Secret Access Key for authentication. Leave empty when running on EC2/ECS with IAM roles. For DynamoDB Local testing, you can use any dummy value like 'dummy'.",
			type = "text"
		)

		,group("Custom Endpoint","Configure region and custom endpoints. Use custom endpoints for DynamoDB Local testing or VPC endpoints.")
		,field(displayName = "Region",
			name = "region",
			defaultValue = "",
			required = false,
			description = "AWS region where the DynamoDB table is located (e.g., 'us-east-1', 'eu-west-1'). If not specified, the default region from your AWS configuration will be used.",
			type = "text"
		)
		,field(displayName = "Host",
			name = "host",
			defaultValue = "",
			required = false,
			description = "Custom endpoint URL for DynamoDB. Use 'http://localhost:8000' for DynamoDB Local testing. Leave empty to use the standard AWS DynamoDB endpoints. Do not include the protocol scheme if using AWS endpoints.",
			type = "text"
		)
		

		,group("General","General cache configuration settings")
		,field(displayName = "Log",
			name = "log",
			defaultValue = "application",
			required = true,
			description = "Name of a existing log where cache operations and errors will be recorded. Set to 'application' to use the default application log, or specify a custom log name (e.g., 'dynamodb-cache').",
			type = "text"
		)

	];

	public string function getClass() {
		return "{class}";
	}

	public string function getLabel() {
		return "{label}";
	}

	public string function getDescription() {
		return "{description}";
	}
}