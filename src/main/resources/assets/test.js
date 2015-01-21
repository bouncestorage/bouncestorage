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
    $scope.status = {};
    $scope.handles = {};

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
                $scope.actions.status();
            }
        );
    };

    $scope.actions.status = function() {
        $timeout.cancel($scope.handles.status);

        $http.get("/bounce").then(
            function(res) {
                $scope.status = res.data;
                if (!$scope.status.every(function(status) { return status.done; })) {
                    $scope.handles.status = $timeout($scope.actions.status, 1 * 1000/*ms*/);
                }
            }
        );
    };
    $scope.actions.status();
}]);

bounce.controller('BounceConfig',
               ['$scope', '$http', '$q', '$location', '$timeout',
function ($scope, $http, $q, $location, $timeout)
{
    $scope.options = {};
    $scope.actions = {};
    $scope.status = {};

    $http.get("/config").then(
        function(res) {
            $scope.config = res.data;
        }
    );

    $scope.actions.config = function() {
        $http.post("/config", $scope.options.config).then(
            function(res) {
                $scope.status.config = res.data;
            }
        );
    };
}]);
