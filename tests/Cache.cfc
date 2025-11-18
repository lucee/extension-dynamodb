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
    variables.cacheName="dynamodb";
    function beforeAll() {
        // TODO find the version automatically
        application action="update" caches = {
            "dynamodb" : {
                "class": 'org.lucee.extension.aws.dynamodb.DynamoDBCache'
                , "maven": 'org.lucee:dynamodb:1.0.0.0-ALPHA'
                , "storage": false
                , "custom": {
                    "table":"test",
                    "accessKeyId":"dummy",
                    "secretkey":"dummy",
                    "region":"us-east-1",
                    "liveTimeout":3600000,
                    "log":"application"
            }
            , default: ''
					}};
    }

    function run( testResults, testBox ) {
        describe( "Testcase for LDEV-3880",function() {
            it( title="Checking cfzip filter delimiters", body=function( currentSpec ) {
                cachePut(id:'abc', value:'AAA', cacheName:variables.cacheName);
		        var val=cacheget(id:'abc', cacheName:variables.cacheName);

                expect(isNull(val)).toBeFalse();
                expect(val).toBe("AAA");
            });
        });
    }

    function afterAll() {
        
    }
} 
</cfscript>
