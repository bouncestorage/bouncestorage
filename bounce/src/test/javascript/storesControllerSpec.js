describe ('Test viewStoresCtrl', function() {
  beforeEach(module('bounce'));

  var viewStoreCtrl, scope, routeParams;

  beforeEach(inject(function ($rootScope, $controller) {
    scope = $rootScope.$new();
    ViewStoresCtrl = $controller('ViewStoresCtrl', { $scope: scope });
  }));

  it ('should return an empty list of containers if blobstore is not set',
    function() {
      scope.editLocation = { object: { blobStoreId: -1, containerName: '' } };
      scope.containersMap = {};
      expect(scope.getContainersForPrompt()).toEqual([]);
  });

  it ('should return only the configured container for prompts', function() {
    scope.editLocation = { object: { blobStoreId: 0,
                                     containerName: 'container' }
                         };
    scope.containersMap = { 0: [ { name: 'container', status: 'INUSE' },
                                 { name: 'other', status: 'UNCONFIGURED' }
                               ]};
    expect(scope.getContainersForPrompt()).toEqual([scope.containersMap[0][0]]);
    expect(scope.editLocation.configured).toEqual(true);
  });

  it ('should return all unconfigured containers for prompts if none are set',
    function() {
      scope.editLocation = { object: { blobStoreId: 0, containerName: '' } };
      scope.enhanceContainer = { originLocation: { blobStoreId: 10 } };
      scope.containersMap = { 0: [ { name: 'container', status: 'INUSE' },
                                   { name: 'other', status: 'UNCONFIGURED' },
                                   { name: 'foo', status: 'UNCONFIGURED' },
                                 ]};
      expected = scope.containersMap[0].filter(function(container) {
        return container.status === 'UNCONFIGURED';
      });
      expect(scope.getContainersForPrompt()).toEqual(expected);
  });

  it ('should fail validation if capacity and move are set', function() {
    tier = { object: { blobStoreId: 0,
                       containerName: 'test',
                       moveDelay: 'P1D',
                       capacity: 1000
           } };
    expect(BounceUtils.validateTier(tier)).toNotEqual(null);
  });

  it ('should fail validation if no policy settings are set', function() {
    tier = { object: { blobStoreId: 0,
                       containerName: 'test',
           } };
    expect(BounceUtils.validateTier(tier)).toNotEqual(null);
  });

  it ('should pass validation if only copy is set', function() {
    tier = { object: { blobStoreId: 0,
                       containerName: 'test',
                       copyDelay: 'P1D'
           } };
    expect(BounceUtils.validateTier(tier)).toEqual(null);
  });

  it ('should pass validation if only move is set', function() {
    tier = { object: { blobStoreId: 0,
                       containerName: 'test',
                       moveDelay: 'P1D'
           } };
    expect(BounceUtils.validateTier(tier)).toEqual(null);
  });

  it ('should pass validation if only move is set', function() {
    tier = { object: { blobStoreId: 0,
                       containerName: 'test',
                       capacity: 1000
           } };
    expect(BounceUtils.validateTier(tier)).toEqual(null);
  });

  it ('should pass if move and copy are set to never and capacity is set',
    function() {
      tier = { object: { blobStoreId: 0,
                        containerName: 'test',
                        capacity: 1000,
                        moveDelay: '-P1D',
                        copyDelay: '-P1D'
            } };
     expect(BounceUtils.validateTier(tier)).toEqual(null);
  });

  it ('should reset changes for canceled edits', function() {
    var modal = jasmine.createSpyObj('modal', ['modal']);
    scope.editModal = modal;
    scope.editLocation = { object: { blobStoreId: 5,
                                     containerName: 'bucket' 
                                   } };
    scope.actions.cancelEditTier(modal);
    expect(scope.editLocation.object.blobStoreId).toEqual(-1);
    expect(scope.editLocation.object.containerName).toEqual('');
    expect(modal.modal).toHaveBeenCalledWith('hide');
  });
});
