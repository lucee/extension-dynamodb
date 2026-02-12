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
        describe( "DynamoDB GetItem Action", function() {
            
            it( "retrieves complex nested structures", function() {
                var pk = "get_1";
                var original = { 
                    "pk": pk, 
                    "tags": ["cfml", "aws"], 
                    "meta": { "version": 1.1, "prod": false } 
                };
                
                DynamoDBCommand("putItem", original, variables.cacheName);
                
                var result = DynamoDBCommand("getItem", { "pk": pk }, variables.cacheName);
                
                expect( isStruct(result) ).toBe( true );
                expect( result.tags[2] ).toBe( "aws" );
                expect( result.meta.prod ).toBe( false );
            });

            it( "returns null for non-existent keys", function() {
                var result = DynamoDBCommand("getItem", { "pk": "ghost" }, variables.cacheName);
                expect( isNull(result) ).toBe( true );
            });
        });
    }
}
</cfscript>