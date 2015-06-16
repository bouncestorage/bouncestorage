describe ('Test createStoreCtrl', function() {
  beforeEach(module('bounce'));

  var CreateStoreCtrl, scope, routeParams;

  beforeEach(inject(function ($rootScope, $controller) {
    scope = $rootScope.$new();
    CreateStoreCtrl = $controller('CreateStoreCtrl',
      { $scope: scope });
  }));  

  it ('should set edit', function() {
    expect(scope.edit).toEqual(undefined);
  });
});

describe ('Test createStoreCtrl -- edit', function() {
  beforeEach(module('bounce'));

  var CreateStoreCtrl, scope;
  var ObjectStore = {};
  var routeParams = { objectStoreId: '1' };
  var mockProvider = {
    id: routeParams.objectStoreId,
    nickname: 'test mock',
    identity: 'test',
    credential: 'test',
    region: 'US',
    endpoint: 'http://endpoint.test',
    storageClass: 'COLD',
    provider: 'google-cloud-storage'
  };

  beforeEach(inject(function ($rootScope, $controller) {
    scope = $rootScope.$new(); 

    ObjectStore.get = function(params, success) {
      if (params.id !== undefined) {
        success(mockProvider);
      }
    };

    CreateStoreCtrl = $controller('CreateStoreCtrl',
      { $scope: scope,
        $routeParams: routeParams,
        ObjectStore: ObjectStore
      });
  }));

  it ('should set edit', function() {
    expect(scope.edit).toEqual(true);
  });

  it ('should set the expected provider values', function() {
    var fields = [ 'nickname', 'identity', 'credential', 'region', 'endpoint'];
    expect(scope.provider.value).toEqual(mockProvider.provider);
    expect(scope.provider.class).toEqual(mockProvider.storageClass);
    for (var i = 0; i < fields; i++) {
      expect(scope.provider[fields[i]]).toEqual(mockProvider[fields[i]]);
    }
  });
});
