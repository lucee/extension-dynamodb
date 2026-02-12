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
        describe( "DynamoDB DeleteItem Action", function() {
            
            it( "removes an item and confirms with ALL_OLD", function() {
                var pk = "del_1";
                DynamoDBCommand("putItem", { "pk": pk, "data": "bye" }, variables.cacheName);
                
                var result = DynamoDBCommand("deleteItem", { 
                    "pk": pk, 
                    "ReturnValues": "ALL_OLD" 
                }, variables.cacheName);
                
                expect( isStruct(result) ).toBe( true );
                expect( result.data ).toBe( "bye" );
                
                // Verify it is gone
                var check = DynamoDBCommand("getItem", { "pk": pk }, variables.cacheName);
                expect( isNull(check) ).toBe( true );
            });
        });
    }
}
</cfscript>