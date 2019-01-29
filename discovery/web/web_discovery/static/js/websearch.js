document.getElementById('search-form').addEventListener('submit', function(e) {
  e.preventDefault();

  var keywords = document.getElementById('keywords').value;
  alert(keywords);
});
