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
        describe( "DynamoDB Query & Scan Actions", function() {
            
            it( "filters table-wide using scan", function() {
                var prefix = "scan_" & createUniqueId();
                DynamoDBCommand("putItem", { "pk": prefix & "_1", "cat": "A" }, variables.cacheName);
                DynamoDBCommand("putItem", { "pk": prefix & "_2", "cat": "B" }, variables.cacheName);
                
                var results = DynamoDBCommand("scan", {
                    "FilterExpression": "cat = :c",
                    "ExpressionAttributeValues": { ":c": "A" }
                }, variables.cacheName);
                
                expect( isArray(results) ).toBe( true );
                expect( arrayLen(results) ).toBe( 1 );
                expect( results[1].pk ).toBe( prefix & "_1" );
            });

            it( "queries specifically by partition key", function() {
                var pk = "query_pk_1";
                DynamoDBCommand("putItem", { "pk": pk, "val": "x" }, variables.cacheName);
                
                var results = DynamoDBCommand("query", {
                    "KeyConditionExpression": "pk = :p",
                    "ExpressionAttributeValues": { ":p": pk }
                }, variables.cacheName);
                
                expect( arrayLen(results) ).toBe( 1 );
                expect( results[1].val ).toBe( "x" );
            });
        });
    }
}
</cfscript>