describe ('Test objectStoreStats', function() {
  beforeEach(module('bounce'));

  var objectStoreStats, scope, $httpBackend;

  beforeEach(inject(function($injector) {
    $httpBackend = $injector.get('$httpBackend');
    scope = $injector.get('$rootScope').$new();
    // Since objectStoreStats is a factory, it returns a function that creates
    // the object.
    objectStoreStats = $injector.get('objectStoreStats');
  }));

  afterEach(function() {
    $httpBackend.verifyNoOutstandingExpectation();
    $httpBackend.verifyNoOutstandingRequest();
  });

  it('should request cumulative stats and ALL PUT/GET for the provider',
    function() {
      var objectStores = [
          { id: 1 // All statistics are referenced by ID
          }
      ];

      var objectStoreId = objectStores[0].id;
      // Summary stats
      $httpBackend.expect('GET',
          constructDBQuery(BounceUtils.objectStoreStatsQuery(
              objectStoreId)))
          .respond([]);
      checkTotalContainerStats(objectStoreId, '.*', null, null, null);
      objectStoreStats.getStats(objectStores);
      $httpBackend.flush();
  });

  it('should augment stats for the container after reported time', function() {
    var objectStores = [
        { id: 1, // All statistics are referenced by ID
          nickname: 'store'
        }
    ];

    var objectStoreId = objectStores[0].id;
    var container = { name: 'foo',
                      time: 1000,
                      size: 1024*40
                    };

    // Summary stats
    $httpBackend.expect('GET',
        constructDBQuery(BounceUtils.objectStoreStatsQuery(
            objectStoreId)))
        .respond([
          { name: 'object_store_stats.provider.' + objectStoreId +
                  '.container.' + container.name,
            points: [[ container.time /*time*/, 1, container.size /*size*/,
                59 /*objects*/ ]]
          }
        ]);

    // container stats
    var sincePut = 1024*10;
    var sinceDelete = 1024*30;
    checkTotalContainerStats(objectStoreId, container.name, container.time,
        buildContainerResponse(objectStoreId, container.name, 'PUT', sincePut),
        buildContainerResponse(objectStoreId, container.name, 'DELETE',
            sinceDelete)
    );

    // All other containers
    checkTotalContainerStats(objectStoreId, '.*', null, null, null);
    objectStoreStats.getStats(objectStores);
    $httpBackend.flush();
    var result = objectStoreStats.result;
    expect(result[0].data.size).toEqual(
        container.size + sincePut - sinceDelete);
  });

  it('should not double count containers from the summary', function() {
    var objectStores = [
        { id: 1, // All statistics are referenced by ID
          nickname: 'store'
        }
    ];

    var objectStoreId = objectStores[0].id;
    var container = { name: 'foo',
                      time: 1000,
                      size: 1024*40
                    };

    // Summary stats
    $httpBackend.expect('GET',
        constructDBQuery(BounceUtils.objectStoreStatsQuery(
            objectStoreId)))
        .respond([
          { name: 'object_store_stats.provider.' + objectStoreId +
                  '.container.' + container.name,
            points: [[ container.time /*time*/, 1, container.size /*size*/,
                59 /*objects*/ ]]
          }
        ]);

    // container stats
    var sincePut = 1024*10;
    var sinceDelete = 1024*30;
    checkTotalContainerStats(objectStoreId, container.name, container.time,
        buildContainerResponse(objectStoreId, container.name, 'PUT', sincePut),
        buildContainerResponse(objectStoreId, container.name, 'DELETE',
            sinceDelete)
    );

    // All other containers
    checkTotalContainerStats(objectStoreId, '.*', null,
        buildContainerResponse(objectStoreId, container.name, 'PUT', sincePut),
        buildContainerResponse(objectStoreId, container.name, 'DELETE',
            sinceDelete)
    );
    objectStoreStats.getStats(objectStores);
    $httpBackend.flush();
    var result = objectStoreStats.result;
    expect(result[0].data.size).toEqual(
        container.size + sincePut - sinceDelete);
  });

  it('should add containers without cumulative statistics', function() {
    var objectStores = [
        { id: 1, // All statistics are referenced by ID
          nickname: 'store'
        }
    ];

    var objectStoreId = objectStores[0].id;
    var container = { name: 'foo',
                      time: 1000,
                      size: 1024*40
                    };

    // Summary stats
    $httpBackend.expect('GET',
        constructDBQuery(BounceUtils.objectStoreStatsQuery(
            objectStoreId)))
        .respond([
          { name: 'object_store_stats.provider.' + objectStoreId +
                  '.container.' + container.name,
            points: [[ container.time /*time*/, 1, container.size /*size*/,
                59 /*objects*/ ]]
          }
        ]);

    // container stats
    var sincePut = 1024*10;
    var sinceDelete = 1024*30;
    checkTotalContainerStats(objectStoreId, container.name, container.time,
        buildContainerResponse(objectStoreId, container.name, 'PUT', sincePut),
        buildContainerResponse(objectStoreId, container.name, 'DELETE',
            sinceDelete)
    );

    // All other containers
    var otherContainer =
        { name: 'other',
          putCount: 1024*1024,
          deleteCount: 1024*64
        };
    checkTotalContainerStats(objectStoreId, '.*', null,
        buildContainerResponse(objectStoreId, otherContainer.name, 'PUT',
            otherContainer.putCount),
        buildContainerResponse(objectStoreId, otherContainer.name, 'DELETE',
            otherContainer.deleteCount)
    );
    objectStoreStats.getStats(objectStores);
    $httpBackend.flush();
    var result = objectStoreStats.result;
    expect(result[0].data.size).toEqual(
        container.size + sincePut - sinceDelete +
        otherContainer.putCount - otherContainer.deleteCount);
  });

  function buildContainerResponse(provider, container, op, value) {
    var serieName = 'ops.provider.' + provider + '.container.' + container +
        '.op.' + op;
    return [ { points: [[ null, null, value]],
               name: serieName
             }];
  }

  function checkTotalContainerStats(objectStoreId, container, since, putValue,
      deleteValue) {
    checkContainerStats(objectStoreId, container, since, putValue, 'PUT');
    checkContainerStats(objectStoreId, container, since,  deleteValue,
        'DELETE');
  }

  function checkContainerStats(id, container, since, value, method) {
    var query = BounceUtils.containerStatsQuery(id, container, method);
    if (since !== null) {
      query += ' where time > ' + since;
    }

    var response = value === null ? [] : value;
    if (value !== null) {
    }
    $httpBackend.expect('GET', constructDBQuery(query)).respond(response);
  }
});
