describe ('Test DashboardCtrl', function() {
  beforeEach(module('bounce'));

  var createController, scope, $httpBackend;
  var objectStoreStats =
    { getStats: function(objectStores) {},
      result: function() { return []; }
    };

  function stubOpsQuery() {
    $httpBackend.expect('GET', constructDBQuery(BounceUtils.OPS_QUERY))
        .respond([]);
  }

  function stubDurationQueries() {
    for (var i in BounceUtils.TRACKED_METHODS) {
      $httpBackend.expect('GET',
          constructDBQuery(BounceUtils.durationQuery(i))).respond([]);
    }
  }

  function stubBounceStatsQueries() {
    for (var i in BounceUtils.BOUNCE_METHODS) {
      $httpBackend.expect('GET',
          constructDBQuery(BounceUtils.BOUNCE_METHODS[i].query)).respond([]);
    }
  }

  beforeEach(inject(function($injector) {
    $httpBackend = $injector.get('$httpBackend');
    scope = $injector.get('$rootScope').$new();
    var $controller = $injector.get('$controller');
    var objectStore = $injector.get('ObjectStore');
    createController = function(params) {
      params['$scope'] = scope;
      params['ObjectStore'] = objectStore;
      params['objectStoreStats'] = objectStoreStats;
      return $controller('DashboardCtrl', params);
    };
  }));

  afterEach(function() {
    $httpBackend.verifyNoOutstandingExpectation();
    $httpBackend.verifyNoOutstandingRequest();
  });

  it ('should query object stores and stats', function() {
    $httpBackend.expectGET('/api/object_store').respond([]);
    stubOpsQuery();
    stubDurationQueries();
    stubBounceStatsQueries();
    createController({});
    $httpBackend.flush();
  });

  it ('should collect stats for all of the object stores', function() {
    var objectStores =
        [{ nickname: 'foo',
           provider: 'aws-s3',
           identity: 'identity',
           credential: 'secret',
           id: 1
         },
         { nickname: 'bar',
           provider: 'aws-s3',
           identity: 'other',
           credential: 'other-secret',
           id: 2
         }
        ];
    $httpBackend.expect('GET', '/api/object_store').respond(objectStores);
    stubOpsQuery();
    stubDurationQueries();
    stubBounceStatsQueries();
    spyOn(objectStoreStats, 'getStats');
    createController({});
    $httpBackend.flush();
    expect(scope.objectStores.length).toEqual(2);
    expect(objectStoreStats.getStats).toHaveBeenCalled();
    var args = objectStoreStats.getStats.mostRecentCall.args.length;
    for (var i = 0; i < args.length; i++) {
      expect(args[i]).toEqual(objectStores[i]);
    }
  });
});
