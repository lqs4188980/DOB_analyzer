<html>
    <head>
        <title>DOB Query</title>
        <script>
            function getConfirm() { 
                var r = confirm("Do you want to delete all the resolved cases?");
                if (r == true) {
                    window.open('/delete', '_self')
                }
            }
        </script>
    </head>
    <body>
        <form action="query/complaint" method="post">
            Query Resolved Case By Complaint Number: <input name="complaint_number" type="text" /><br />
            <input type="submit" value="Query" />
        </form>
        <br />
        <form action="query/disposition" method="post">
            Query Resolved Case By Disposition Date Range: <br />
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

        <br />
        <br />
        <input type="button" value="Delete All Resolved Cases" id="delete" onclick="getConfirm()" />

    </body>
</html>
