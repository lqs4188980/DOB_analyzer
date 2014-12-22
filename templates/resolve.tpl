<html>
    <head>
        <title>Result</title>
        <script type='text/javascript' src="/static/script/js/jquery-1.11.1.min.js"></script>
        <script type='text/javascript' src="/static/script/js/jquery.dataTables.min.js"></script>
        <link rel='stylesheet' type="text/css" href="/static/script/css/jquery.dataTables.css">
        <script type='text/javascript'>
            $(document).ready(function() {
                $('#complaint').DataTable();
            });
        </script>
        <style type="text/css">
            table, td, th {
                border: 1px solid black;
                text-align: center;
            } 
        </style>
    </head>

    <body>
        <input type="button" value="Return to Home" id="back" onclick="window.open('/','_self')" />
        <table id='complaint' style="width: 100%;">
            <thead> 
                <tr>
                    <th>Complaint Number</th>
                    <th>Status</th>
                    <th>BIN</th>
                    <th>Category Code</th>
                    <th>Lot</th>
                    <th>Block</th>
                    <th>ZIP</th>
                    <th>Received</th>
                    <th>DOB Violation #</th>
                    <th>Comments</th>
                    <th>Owner</th>
                    <th>Last Inspection</th>
                    <th>Borough</th>
                    <th>Complaint at</th>
                    <th>ECB Violation #s</th>
                </tr>
            </thead>

            <tfoot> 
                <tr>
                    <th>Complaint Number</th>
                    <th>Status</th>
                    <th>BIN</th>
                    <th>Category Code</th>
                    <th>Lot</th>
                    <th>Block</th>
                    <th>ZIP</th>
                    <th>Received</th>
                    <th>DOB Violation #</th>
                    <th>Comments</th>
                    <th>Owner</th>
                    <th>Last Inspection</th>
                    <th>Borough</th>
                    <th>Complaint at</th>
                    <th>ECB Violation #s</th>
                </tr>
            </tfoot>

            <tbody>
            % for info in infoList:
                <tr>
                    <td>{{info['Complaint Number']}}</td>
                    <td>{{info['Status']}}</td>
                    <td>{{info['BIN']}}</td>
                    <td>{{info['Category Code']}}</td>
                    <td>{{info['Lot']}}</td>
                    <td>{{info['Block']}}</td>
                    <td>{{info['ZIP']}}</td>
                    <td>{{info['Received']}}</td>
                    <td>{{info['DOB Violation #']}}</td>
                    <td>{{info['Comments']}}</td>
                    <td>{{info['Owner']}}</td>
                    <td>{{info['Last Inspection']}}</td>
                    <td>{{info['Borough']}}</td>
                    <td>{{info['Complaint at']}}</td>
                    <td>{{info['ECB Violation #']}}</td>
                </tr>
            % end
            </tbody>
        </table>
    </body>
</html>
