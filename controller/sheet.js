var API_url = "";

// this script should be implemented in the Google App Script of sheet with format like GroupSchedule sheet.

function myFunction() {
    // Get column index by column name.
    var ss = SpreadsheetApp.getActiveSpreadsheet();
    var sheet = ss.getSheetByName('task');
    var range = sheet.getRange('A:J');
    var data = range.getValues();
    var header = data[0];
    var output_arr = new Array();
    var offer_id_index = header.indexOf('offer_id');
    var group_name_index = header.indexOf('group_name');
    var percent_index = header.indexOf('percent');
    var rate_index = header.indexOf('rate');
    var actived_from_index = header.indexOf('actived_from');
    var actived_to_index = header.indexOf('actived_to');
    var email_index = header.indexOf('email');
    var status_index = header.indexOf('status')
    var note_index = header.indexOf('note');
    var force_resync_at_index = header.indexOf('force_resync_at');
    
    for(var i = 0; i < data.length; i++) {
        // get status with `created` and `cancel` row 
        // TODO : status `cancel` logic hasn't implemented yet, should be able to cancel the scheduled task. 
        if (data[i][status_index] == 'created' || data[i][status_index] == 'cancel') {
            data[i].push(i.toString())
            output_arr.push({
            'offer_id' : data[i][offer_id_index],
            'group_name' : data[i][group_name_index],
            'percent' : data[i][percent_index],
            'rate' : data[i][rate_index],
            'actived_from' : data[i][actived_from_index],
            'actived_to' : data[i][actived_to_index],
            'status' : data[i][status_index],
            'force_resync_at' : data[i][force_resync_at_index],
            'index' : i + 1
            })
        }
    }
    var grouped = groupBy(output_arr, function(item) {
        // group by selected row with offer_id, actived_from, actived_to, which each group should be considered as per task.
        return[item.offer_id, item.actived_from, item.actived_to]
    })
    
    for(var g = 0; g < grouped.length; g ++) {
        // send each group to API Gateway for processing.
        var start_t = new Date();
        var mail_body = '\n';
        var mail_address = "";
        var response = JSON.parse(lambda_process(grouped[g]))
        var end_t = new Date();
        var diff = (end_t - start_t)/1000;
        for(var r = 0; r < response.length; r ++ ) {
            var index = response[r].index;
            var target_row = sheet.getRange('A'+index+':I'+index);
            var old = target_row.getValues()[0];
            if (response[r].status) {
                old[status_index] = 'saved'
                }
            else {
                old[status_index] = 'failed'
                old[note_index] = response[r].note;
            }
            target_row.setValues([old]); 
            mail_body += ('Response of offer_id ' + old[offer_id_index] + ' group - ' + old[group_name_index] + ' : ' + JSON.stringify(response[r]) + '\n');
            mail_address = old[email_index];
        }
        mail_body += diff.toString() + 'sec';
        try {
            MailApp.sendEmail(mail_address, 'GroupSchedule Process Response', mail_body)
        } catch(err) {
            
            }
        }
  }
                          
  function groupBy( array , f ) {
        var groups = {};
            array.forEach( function( o ) {
                var group = JSON.stringify( f(o) );
                groups[group] = groups[group] || [];
                groups[group].push( o );
            });
            return Object.keys(groups).map( function( group ) {
                return groups[group];
        });
  }
  function lambda_process( data ) {
      
    var options = { 
        'method': 'post', 
        'contentType': 'application/json', 
        'payload' : JSON.stringify(data) 
    };
    console.log(options)
    try {
        var response = UrlFetchApp.fetch(API_url, options);
        return response.getContentText()
    } 
    catch(err) {
        throw new Error(err); 
    }
}
  
  