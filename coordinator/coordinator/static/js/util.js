function toggle_visibility(id) {
  var e = document.getElementById(id);
  if(e.style.display == 'block')
    e.style.display = 'none';
  else
    e.style.display = 'block';
}

if(!Object.entries) {
  Object.entries = function(obj) {
    var ownProps = Object.keys(obj),
      i = ownProps.length,
      resArray = new Array(i); // preallocate the Array
    while(i--) {
      resArray[i] = [ownProps[i], obj[ownProps[i]]];
    }

    return resArray;
  };
}

function loadTemplate(elementId) {
  // Load template from element
  var tpl = document.getElementById(elementId).innerHTML;

  // Return a function to render that template
  return (function(appendToElement, args) {
    // Replace placeholders with values from argument
    args = Object.entries(args);
    var html = tpl;
    for(var i = 0; i < args.length; ++i) {
      html = html.replace(new RegExp('__' + args[i][0] + '__', 'g'), '' + args[i][1]);
    }

    if(appendToElement !== null) {
      // Turn the generated HTML into element(s)
      var new_div = document.createElement('div');
      appendToElement.appendChild(new_div);
      new_div.outerHTML = html;
    } else {
      return html;
    }
  });
}
