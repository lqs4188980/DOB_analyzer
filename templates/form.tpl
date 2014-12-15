<html>
    <head>
	<title>DOB Query</title>
    </head>
    <body>
	<form action="query_complaint" method="post">
	    Query Resolved Case By Complaint Number: <input name="complaint_number" type="text" /><br />
	    <input type="submit" value="Query" />
	</form>
        <br />
        <form action="query_daterange" method="post">
            Query Resolved Case By Last Inspection Date Range: <br />
            Start Date: <input type="date" name="startDate" /><br />
            End Date: <input type="date" name="endDate" /><br />
            <input type="submit" value="Query" />
        </form>
	<input type="button" value="Export All Resolved Case" id="export" onclick="window.open('/export','_self')" />
    </body>
</html>
