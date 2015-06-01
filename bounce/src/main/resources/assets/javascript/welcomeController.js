/*global angular*/

var welcomeControllers = angular.module('welcomeControllers', []);

welcomeControllers.controller('WelcomeCtrl', ['$location', 'ObjectStore',
  function ($location, ObjectStore) {
    ObjectStore.query(function(results) {
      if (results.length > 0) {
        $location.path('/dashboard');
      }
    });
}]);
