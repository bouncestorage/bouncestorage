var bounce = angular.module('bounce', [
  'ngRoute',
  'welcomeControllers'
]);

bounce.config(['$locationProvider', function($locationProvider) {
    $locationProvider.html5Mode(false);
}]);

bounce.config(['$routeProvider', function($routeProvider) {
  $routeProvider.
    when('/welcome', {
      templateUrl: 'views/partials/welcome.html',
      controller: 'WelcomeCtrl'
    }).
    otherwise({
      redirectTo: '/welcome'
    });
}]);
