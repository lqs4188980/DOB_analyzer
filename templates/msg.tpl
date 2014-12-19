<html>
    <head>
        <title>Message</title>
    </head>
    <body>
        <input type="button" value="Return to Home" id="back" onclick="window.open('/', '_self')" /><br />
        <p>{{message}}</p>
        % if filePath != "":
        <a href="{{filePath}}">Click to Download</a>
        % end
    </body>
</html>
