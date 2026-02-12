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
}
</cfscript>