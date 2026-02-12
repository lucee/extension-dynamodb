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
component extends="Abstr" labels="dynamodb" {
    
    function run( testResults, testBox ) {
        describe( "DynamoDB UpdateItem Action", function() {
            
            it( "performs atomic increments", function() {
                var pk = "upd_1";
                DynamoDBCommand("putItem", { "pk": pk, "hits": 10 }, variables.cacheName);
                
                var result = DynamoDBCommand("updateItem", {
                    "key": { "pk": pk },
                    "UpdateExpression": "SET hits = hits + :inc",
                    "ExpressionAttributeValues": { ":inc": 5 },
                    "ReturnValues": "UPDATED_NEW"
                }, variables.cacheName);
                
                expect( result.hits ).toBe( 15 );
            });

            it( "handles reserved words using ExpressionAttributeNames", function() {
                var pk = "upd_2";
                DynamoDBCommand("putItem", { "pk": pk, "status": "init" }, variables.cacheName);
                
                var result = DynamoDBCommand(
                    "updateItem", {
                        "key": { "pk": pk },
                        "UpdateExpression": "SET ##s = :val",
                        "ExpressionAttributeNames": { "##s": "status" },
                        "ExpressionAttributeValues": { ":val": "active" },
                        "ReturnValues": "UPDATED_NEW"
                    }, 
                    variables.cacheName
                );
                
                expect( result.status ).toBe( "active" );
            });
        });
    }
}
</cfscript>