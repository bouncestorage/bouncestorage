var bounce = angular.module('bounce', [])
    .config(['$locationProvider',
function($locationProvider) {
    $locationProvider.html5Mode(false).hashPrefix('!');
}]);

bounce.controller('BounceTest',
               ['$scope', '$http', '$q', '$location', '$timeout',
function ($scope, $http, $q, $location, $timeout)
{
    $scope.actions = {};
    $scope.options = {};

    $http.get("/service").then(
        function(res) {
            $scope.buckets = res.data.containerNames;
            if (!$scope.options.bucketSelect && $scope.buckets) {
                $scope.options.bucketSelect = $scope.buckets[0];
            }
        }
    );

    $scope.actions.listBlobs = function() {
        $http.get("/container?name=" + $scope.options.bucketSelect).then(
            function(res) {
                $scope.blobs = {}
                $scope.blobs.names = res.data.blobNames;
                $scope.blobs.linkCount = res.data.bounceLinkCount;
            }
        );
    };

    $scope.actions.bounceBlobs = function() {
        $http.post("/bounce?name=" + $scope.options.bucketSelect).then(
            function(res) {
                console.log("Bounce blobs response received");
            }
        );
    }
}]);

