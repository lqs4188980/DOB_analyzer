<html>
    <head>
        <title>DOB Query</title>
    </head>
    <body>
        <form action="query/complaint" method="post">
            Query Resolved Case By Complaint Number: <input name="complaint_number" type="text" /><br />
            <input type="submit" value="Query" />
        </form>
        <br />
        <form action="query/inspection" method="post">
            Query Resolved Case By Last Inspection Date Range: <br />
            Start Date: <input type="date" name="startDate" /><br />
            End Date: <input type="date" name="endDate" /><br />
            <input type="submit" value="Query" />
        </form>
        <br />
        <form action="query/category" method="post">
            Category: <input type="text" name="category" /><br />
            <input type="submit" value="Query" />
        </form>
        <input type="button" value="Export All Resolved Case" id="export" onclick="window.open('/export','_self')" />
        <input type="button" value="Clear Generated Files" id="clearup" onclick="window.open('/clearup', '_self')" />
    </body>
</html>
