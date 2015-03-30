var bounce = angular.module('bounce', [
  'dashboardControllers',
  'ngResource',
  'ngRoute',
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
    when('/edit_store/:objectStore', {
      redirectTo: '/create_store/false/:objectStore'
    }).
    when('/dashboard', {
      templateUrl: 'views/partials/dashboard.html',
      controller: 'DashboardCtrl'
    }).
    when('/stores', {
      templateUrl: 'views/partials/stores.html',
      controller: 'ViewStoresCtrl'
    }).
    when('/edit_container/:containerId?', {
      redirectTo: 'create_store/false'
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
  return $resource('/api/service');
}]);

bounce.factory('VirtualContainer', ['$resource', function($resource) {
  return $resource('/api/virtual_container');
}]);

bounce.factory('ObjectStore', ['$resource', function($resource) {
  return $resource('/api/object_store/:id', { id: "@id" },
    { 'update': { method: 'PUT',}
    });
}]);
