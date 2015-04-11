var bounce = angular.module('bounce', [
  'ngResource',
  'ngRoute',
  'settingsControllers',
  'storesControllers',
  'virtualContainersControllers',
  'welcomeControllers'
]);

bounce.config(['$locationProvider', function($locationProvider) {
    $locationProvider.html5Mode(false);
}]);

bounce.config(['$routeProvider', function($routeProvider) {
  $routeProvider.
    when('/create_container', {
      templateUrl: 'views/partials/create_container.html',
      controller: 'CreateVirtualContainerCtrl'
    }).
    when('/create_store/:welcomeUrl/:objectStoreId?', {
      templateUrl: 'views/partials/create_store.html',
      controller: 'CreateStoreCtrl'
    }).
    when('/dashboard', {
      templateUrl: 'views/partials/dashboard.html',
      controller: 'ViewContainersCtrl'
    }).
    when('/edit_store/:objectStore', {
      redirectTo: '/create_store/false/:objectStore'
    }).
    when('/edit_container/:containerId', {
      templateUrl: 'views/partials/edit_container.html',
      controller: 'EditVirtualContainerCtrl'
    }).
    when('/stores/:id?', {
      templateUrl: 'views/partials/stores.html',
      controller: 'ViewStoresCtrl'
    }).
    when('/settings', {
      templateUrl: 'views/partials/settings.html',
      controller: 'SettingsCtrl'
    }).
    when('/welcome', {
      templateUrl: 'views/partials/welcome.html',
      controller: 'WelcomeCtrl'
    }).
    otherwise({
      redirectTo: '/welcome'
    });
}]);

bounce.factory('Container', ['$resource', function($resource) {
  return $resource('/api/object_store/:id/container', { id: "@id" });
}]);

bounce.factory('VirtualContainer', ['$resource', function($resource) {
  return $resource('/api/virtual_container/:id', { id: "@id" },
    { 'update': { method: 'PUT'}
    });
}]);

bounce.factory('ObjectStore', ['$resource', function($resource) {
  return $resource('/api/object_store/:id', { id: "@id" },
    { 'update': { method: 'PUT'}
    });
}]);

bounce.factory('BounceService', ['$resource', function($resource) {
  return $resource('/api/bounce/:name', { name: "@name" });
}]);
