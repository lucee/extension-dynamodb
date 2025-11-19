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
    variables.sleepInterval = 10; // ms, used for timing-sensitive operations
    
    function beforeAll() {
        // Get DynamoDB endpoint from environment (for testing with DynamoDB Local)
        var dynamoHost = server.system.environment.DYNAMODB_ENDPOINT ?: "http://localhost:8000";
        var accessKey = server.system.environment.AWS_ACCESS_KEY_ID ?: "dummy";
        var secretKey = server.system.environment.AWS_SECRET_ACCESS_KEY ?: "dummy";
        var region = server.system.environment.AWS_REGION ?: "us-east-1";
        
        // Configure DynamoDB cache
        application action="update" caches = {
            "dynamodb" : {
                "class": 'org.lucee.extension.aws.dynamodb.DynamoDBCache',
                "maven": 'org.lucee:dynamodb:1.0.0.0-ALPHA',
                "storage": false,
                "custom": {
                    "table": "test",
                    "accessKeyId": accessKey,
                    "secretkey": secretKey,
                    "region": region,
                    "host": dynamoHost,
                    "liveTimeout": 3600000,
                    "log": "application"
                },
                "default": ""
            }
        };
    }

    function run( testResults, testBox ) {
        describe( "DynamoDB Cache Tests", function() {
            
            // Test basic put, exists, get, delete operations
            it( title="test cachePut, cacheIdExists, cacheGet, cacheDelete with string value", body=function( currentSpec ) {
                var key = "str_" & hash(createUniqueId() & ":" & server.lucee.version, "quick");
                var val = "Test String Value";
                
                try {
                    cachePut(id=key, value=val, cacheName=variables.cacheName);
                    sleep(variables.sleepInterval);
                    
                    expect(cacheIdExists(id=key, cacheName=variables.cacheName)).toBe(true);
                    expect(cacheGet(id=key, cacheName=variables.cacheName)).toBe(val);
                    
                    cacheDelete(id=key, cacheName=variables.cacheName);
                    sleep(variables.sleepInterval);
                    
                    expect(cacheIdExists(id=key, cacheName=variables.cacheName)).toBe(false);
                } finally {
                    // Cleanup in case of failure
                    try { cacheDelete(id=key, cacheName=variables.cacheName); } catch(any e) {
                        debug(e);
                    }
                }
            });

            // Test numeric values
            it( title="test caching numeric values", body=function( currentSpec ) {
                var key = "num_" & hash(createUniqueId() & ":" & server.lucee.version, "quick");
                var val = 12345.67;
                
                try {
                    cachePut(id=key, value=val, cacheName=variables.cacheName);
                    sleep(variables.sleepInterval);
                    
                    var cachedVal = cacheGet(id=key, cacheName=variables.cacheName);
                    expect(isNull(cachedVal)).toBe(false);
                    expect(isNumeric(cachedVal)).toBe(true);
                    expect(cachedVal).toBe(val);
                } finally {
                    try { cacheDelete(id=key, cacheName=variables.cacheName); } catch(any e) {}
                }
            });

            // Test date values
            it( title="test caching date values", body=function( currentSpec ) {
                var key = "date_" & hash(createUniqueId() & ":" & server.lucee.version, "quick");
                var val = now();
                
                try {
                    cachePut(id=key, value=val, cacheName=variables.cacheName);
                    sleep(variables.sleepInterval);
                    
                    var cachedVal = cacheGet(id=key, cacheName=variables.cacheName);
                    expect(isNull(cachedVal)).toBe(false);
                    expect(isDate(cachedVal)).toBe(true);
                } finally {
                    try { cacheDelete(id=key, cacheName=variables.cacheName); } catch(any e) {}
                }
            });

            // Test struct values
            it( title="test caching struct values", body=function( currentSpec ) {
                var key = "struct_" & hash(createUniqueId() & ":" & server.lucee.version, "quick");
                var val = {
                    "name": "John Doe",
                    "age": 30,
                    "active": true,
                    "nested": {"city": "New York"}
                };
                
                try {
                    cachePut(id=key, value=val, cacheName=variables.cacheName);
                    sleep(variables.sleepInterval);
                    
                    var cachedVal = cacheGet(id=key, cacheName=variables.cacheName);
                    expect(isNull(cachedVal)).toBe(false);
                    expect(isStruct(cachedVal)).toBe(true);
                    expect(structKeyExists(cachedVal, "name")).toBe(true);
                    expect(cachedVal.name).toBe("John Doe");
                    expect(cachedVal.nested.city).toBe("New York");
                } finally {
                    try { cacheDelete(id=key, cacheName=variables.cacheName); } catch(any e) {}
                }
            });

            // Test array values
            it( title="test caching array values", body=function( currentSpec ) {
                var key = "array_" & hash(createUniqueId() & ":" & server.lucee.version, "quick");
                var val = [1, "two", 3.0, true, {"nested": "value"}];
                
                try {
                    cachePut(id=key, value=val, cacheName=variables.cacheName);
                    sleep(variables.sleepInterval);
                    
                    var cachedVal = cacheGet(id=key, cacheName=variables.cacheName);
                    expect(isNull(cachedVal)).toBe(false);
                    expect(isArray(cachedVal)).toBe(true);
                    expect(arrayLen(cachedVal)).toBe(5);
                    expect(cachedVal[1]).toBe(1);
                    expect(cachedVal[2]).toBe("two");
                } finally {
                    try { cacheDelete(id=key, cacheName=variables.cacheName); } catch(any e) {}
                }
            });

            // Test cacheGetAll and cacheGetAllIds with filter
            it( title="test cacheGetAll and cacheGetAllIds with filter", body=function( currentSpec ) {
                var prefix = "filter_test_" & hash(createUniqueId(), "quick") & "_";
                var keys = [prefix & "1", prefix & "2", prefix & "3"];
                
                try {
                    // Put multiple items
                    cachePut(id=keys[1], value="value1", cacheName=variables.cacheName);
                    cachePut(id=keys[2], value="value2", cacheName=variables.cacheName);
                    cachePut(id=keys[3], value="value3", cacheName=variables.cacheName);
                    sleep(variables.sleepInterval);
                    
                    // Test cacheGetAllIds with filter
                    var allIds = cacheGetAllIds(filter=prefix & "*", cacheName=variables.cacheName);
                    expect(isArray(allIds)).toBe(true);
                    expect(arrayLen(allIds)).toBeGTE(3); // At least 3
                    
                    // Test cacheGetAll with filter
                    var allValues = cacheGetAll(filter=prefix & "*", cacheName=variables.cacheName);
                    expect(isStruct(allValues)).toBe(true);
                    expect(structCount(allValues)).toBeGTE(3); // At least 3
                    
                    // Verify specific key exists in results
                    expect(structKeyExists(allValues, keys[1])).toBe(true);
                    expect(allValues[keys[1]]).toBe("value1");
                    
                }
                finally {
                    // Cleanup
                    loop array=keys item="local.k" {
                        try { cacheDelete(id=k, cacheName=variables.cacheName); } catch(any e) {}
                    }
                }
            });

            // Test cache expiration with timeSpan
            it( title="test cache expiration with timeSpan", body=function( currentSpec ) {
                var key = "expire_" & hash(createUniqueId() & ":" & server.lucee.version, "quick");
                var val = "Temporary Value";
                
                try {
                    // Store with 1 second expiration
                    cachePut(id=key, value=val, timeSpan=createTimeSpan(0,0,0,1), cacheName=variables.cacheName);
                    sleep(variables.sleepInterval);
                    
                    // Should exist immediately
                    expect(cacheIdExists(id=key, cacheName=variables.cacheName)).toBe(true);
                    
                    // Wait for expiration (1.5 seconds to be safe)
                    sleep(2000);
                    
                    // Should no longer exist
                    expect(cacheIdExists(id=key, cacheName=variables.cacheName)).toBe(false);
                } finally {
                    try { cacheDelete(id=key, cacheName=variables.cacheName); } catch(any e) {}
                }
            });

            // Test cacheCount
            it( title="test cacheCount", body=function( currentSpec ) {
                var prefix = "count_test_" & hash(createUniqueId(), "quick") & "_";
                var keys = [prefix & "1", prefix & "2", prefix & "3"];
                
                try {
                    var initialCount = cacheCount(cacheName=variables.cacheName);
                    
                    // Add items
                    for (var i = 1; i <= 3; i++) {
                        cachePut(id=keys[i], value="value" & i, cacheName=variables.cacheName);
                    }
                    sleep(variables.sleepInterval);
                    
                    var newCount = cacheCount(cacheName=variables.cacheName);
                    expect(newCount).toBeGTE(initialCount + 3);
                    
                } finally {
                    loop array=keys item="local.k" {
                        try { cacheDelete(id=k, cacheName=variables.cacheName); } catch(any e) {}
                    }
                }
            });

            // Test cacheGetMetadata
            it( title="test cacheGetMetadata", body=function( currentSpec ) {
                var key = "meta_" & hash(createUniqueId() & ":" & server.lucee.version, "quick");
                var val = "Test Metadata Value";
                
                try {
                    cachePut(id=key, value=val, cacheName=variables.cacheName);
                    sleep(variables.sleepInterval);
                    
                    var metadata = cacheGetMetadata(id=key, cacheName=variables.cacheName);
                    expect(isNull(metadata)).toBe(false);
                    expect(isStruct(metadata)).toBe(true);
                    
                } finally {
                    try { cacheDelete(id=key, cacheName=variables.cacheName); } catch(any e) {}
                }
            });

            // Test cacheClear with filter
            it( title="test cacheClear with filter", body=function( currentSpec ) {
                var prefix = "clear_test_" & hash(createUniqueId(), "quick") & "_";
                var keys = [prefix & "1", prefix & "2", prefix & "3"];
                
                try {
                    // Add items
                    for (var i = 1; i <= 3; i++) {
                        cachePut(id=keys[i], value="value" & i, cacheName=variables.cacheName);
                    }
                    sleep(variables.sleepInterval);
                    
                    // Verify they exist
                    expect(cacheIdExists(id=keys[1], cacheName=variables.cacheName)).toBe(true);
                    
                    // Clear with filter
                    cacheClear(filter=prefix & "*", cacheName=variables.cacheName);
                    sleep(variables.sleepInterval);
                    
                    // Verify they're gone
                    expect(cacheIdExists(id=keys[1], cacheName=variables.cacheName)).toBe(false);
                    expect(cacheIdExists(id=keys[2], cacheName=variables.cacheName)).toBe(false);
                    expect(cacheIdExists(id=keys[3], cacheName=variables.cacheName)).toBe(false);
                    
                } finally {
                    // Already cleared, but just in case
                    loop array=keys item="local.k" {
                        try { cacheDelete(id=k, cacheName=variables.cacheName); } catch(any e) {}
                    }
                }
            });

            // Test cacheGet with throwWhenNotExist
            it( title="test cacheGet with throwWhenNotExist behavior", body=function( currentSpec ) {
                var key = "nonexistent_" & hash(createUniqueId(), "quick");
                
                // Without throw, should return null
                var val = cacheGet(id=key, throwWhenNotExist=false, cacheName=variables.cacheName);
                expect(isNull(val)).toBe(true);
                
                // With throw, should throw exception
                var hasException = false;
                try {
                    cacheGet(id=key, throwWhenNotExist=true, cacheName=variables.cacheName);
                } catch (any e) {
                    hasException = true;
                }
                expect(hasException).toBe(true);
            });

        });
    }

    function afterAll() {
        // Clean up cache configuration
        application action="update" caches={};
    }
} 
</cfscript>