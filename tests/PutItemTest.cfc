<!--- 
*
* Copyright (c) 2016, Lucee Assosication Switzerland. All rights reserved.
*
* This library is free software; you can redistribute it and/or
* modify it under the terms of the GNU Lesser General Public
* License as published by the Free Software Foundation; either 
* version 2.1 of the License, or (at your option) any later version.
* 
* This library is distributed in the hope that it will be useful,
* but WITHOUT ANY WARRANTY; without even the implied warranty of
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
* Lesser General Public License for more details.
* 
* You should have received a copy of the GNU Lesser General Public 
* License along with this library.  If not, see <http://www.gnu.org/licenses/>.
* 
---><cfscript>
component extends="org.lucee.cfml.test.LuceeTestCase" labels="dynamodb" {
    
    variables.cacheName = "dynamodb";

    function run( testResults, testBox ) {
        describe( "DynamoDB PutItem Action", function() {
            
            it( "can put a simple item", function() {
                var item = { "pk": "put_1", "name": "Urs", "active": true };
                var result = DynamoDBCommand("putItem", item, variables.cacheName);
                // By default putItem returns null unless ReturnValues is set
                expect( isNull(result) ).toBe( true );
            });

            it( "can put an item and return old values", function() {
                var pk = "put_2";
                // First put
                DynamoDBCommand("putItem", { "pk": pk, "status": "old" }, variables.cacheName);
                
                // Second put with ReturnValues
                var result = DynamoDBCommand("putItem", { 
                    "pk": pk, 
                    "status": "new", 
                    "ReturnValues": "ALL_OLD" 
                }, variables.cacheName);
                
                expect( isStruct(result) ).toBe( true );
                expect( result.status ).toBe( "old" );
            });
        });
    }

     function beforeAll() {
        // Get DynamoDB endpoint from environment (for testing with DynamoDB Local)
        var dynamoHost = server.system.environment.DYNAMODB_ENDPOINT ?: "http://localhost:8000";
        var accessKey = server.system.environment.AWS_ACCESS_KEY_ID ?: "dummy";
        var secretKey = server.system.environment.AWS_SECRET_ACCESS_KEY ?: "dummy";
        var region = server.system.environment.AWS_REGION ?: "us-east-1";
        var version = server.system.environment.EXTENSION_VERSION;
        // Configure DynamoDB cache
        application action="update" caches = {
            "dynamodb" : {
                "class": 'org.lucee.extension.aws.dynamodb.DynamoDBCache',
                "maven": 'org.lucee:dynamodb:#version#',
                "storage": false,
                "custom": {
                    "table": "test",
                    "accessKeyId": accessKey,
                    "secretkey": secretKey,
                    "region": region,
                    "host": dynamoHost,
                    "primaryKey": "pk",
                    "liveTimeout": 3600000,
                    "log": "application"
                },
                "default": ""
            }
        };
    }

    function afterAll() {
        // Clean up cache configuration
        application action="update" caches={};
    }
}
</cfscript>