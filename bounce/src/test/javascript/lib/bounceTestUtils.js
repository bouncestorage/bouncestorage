function constructDBQuery(query) {
  var encodedQuery = encodeURIComponent(query);
  encodedQuery = encodedQuery.replace(/%24/g, '$');
  encodedQuery = encodedQuery.replace(/%2C/g, ',');
  return BounceUtils.SERIES_URL + "&q=" + encodedQuery.replace(/%20/g, '+');
}
