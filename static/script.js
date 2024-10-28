function openTab(tabName, event) {
    var i;
    var tabContents = document.getElementsByClassName("tabcontent");
    var tabLinks = document.getElementsByClassName("tablink");

    for (i = 0; i < tabContents.length; i++) {
        tabContents[i].style.display = "none"; // Hide all tab content
    }

    for (i = 0; i < tabLinks.length; i++) {
        tabLinks[i].className = tabLinks[i].className.replace(" active", ""); // Remove "active" class from all tabs
    }

    var tabElement = document.getElementById(tabName);
    if (tabElement) {
        tabElement.style.display = "block"; // Show the content of the clicked tab
        tabElement.className += " active"; // Add "active" class to the clicked tab
    } else {
        // console.log("Tab element not found: " + tabName);
        // Optionally, open a default tab here if the desired tab doesn't exist
        // document.getElementById('defaultOpen').click();
    }

    if (event.currentTarget) {
        event.currentTarget.className += " active"; // Add "active" class to the clicked tab
    } else {
        // console.log("Current target element not found.");
    }

    localStorage.setItem('lastOpenedTab', tabName);
}

$('document').ready(function () {
    var openedTab = localStorage.getItem("lastOpenedTab");
    // console.log('Opened Tab: ', openedTab)

    if (openedTab == 'SingleLine') {

        document.getElementById('defaultOpen').classList.add('addedclass');
        document.getElementById('BF').classList.remove('addedclass');
        document.getElementById('userdfc').classList.remove('addedclass');
        // document.getElementById('user').classList.remove('addedclass');
        document.getElementById('MCF').classList.remove('addedclass');
        return;
    }
    else if (openedTab == 'Batch') {
        document.getElementById('BF').classList.add('addedclass');
        document.getElementById('defaultOpen').classList.remove('addedclass');
        document.getElementById('MCF').classList.remove('addedclass');
        document.getElementById('userdfc').classList.remove('addedclass');
        // document.getElementById('user').classList.remove('addedclass');
        return;
    }
    else if (openedTab == 'MapCreationForm') {
        document.getElementById('MCF').classList.add('addedclass');
        document.getElementById('defaultOpen').classList.remove('addedclass');
        document.getElementById('BF').classList.remove('addedclass');
        document.getElementById('userdfc').classList.remove('addedclass');
        // document.getElementById('user').classList.remove('addedclass');
        return;
    }
    else if (openedTab == 'UDComponents') {
        document.getElementById('defaultOpen').classList.remove('addedclass');
        document.getElementById('BF').classList.remove('addedclass');
        document.getElementById('MCF').classList.remove('addedclass');
        // document.getElementById('user').classList.remove('addedclass');
        document.getElementById('userdfc').classList.add('addedclass');
        return;
    }
    else if (openedTab == 'ClueTab') {
        document.getElementById('defaultOpen').classList.remove('addedclass');
        document.getElementById('BF').classList.remove('addedclass');
        document.getElementById('MCF').classList.remove('addedclass');
        document.getElementById('userdfc').classList.remove('addedclass');
        document.getElementById('clue').classList.add('addedclass');
        return;
    }

    // console.log(openedTab);
});
document.addEventListener('DOMContentLoaded', function () {
    var lastOpenedTab = localStorage.getItem('lastOpenedTab');
    var tabElement = document.getElementById(lastOpenedTab);

    if (tabElement) {
        openTab(lastOpenedTab, new Event('click'));
    } else {
        // console.log("Saved tab not found. Opening default tab.");
        document.getElementById('defaultOpen').click();
    }
});

window.addEventListener('load', function () {
    var lastOpenedTab = localStorage.getItem('lastOpenedTab');
    var tabElement = document.getElementById(lastOpenedTab);

    if (tabElement) {
        openTab(lastOpenedTab, new Event('click'));
    } else {
        // console.log("Saved tab not found. Opening default tab.");
        document.getElementById('defaultOpen').click();
    }
});

//------------------------------------------------------------------------------------------------------------
//                                 Single Line Address Parser Tab Functionality
//------------------------------------------------------------------------------------------------------------
$('#defaultOpen').click(function () {
    location.reload(true);
    document.getElementById("defaultOpen").value = ' active';
});


$(document).ready(function () {
    $('#submit-btn').click(function (event) {
        var address = $('#address-input').val().trim();
        if (address === '') {
            event.preventDefault(); // Stop the form from being submitted
            alert('Please enter an address before submitting.');
        }
    });
    isExceptionForced = false;
    var unsatisfied_address = "";

    $('#mapdata').hide();
    $('#result-box').hide()
    $('#myForm').submit(function (event) {
        event.preventDefault();
        $('#result-box').show();
        var addressValue = $('#address-input').val();
        unsatisfied_address = addressValue;
        $.ajax({
            type: "POST",
            url: "/",
            data: { address: addressValue },
            success: function (response) {
                // console.log('Response:', response);
                renderAddressData(response);
                document.getElementById('exception-controls').style.display = 'block';
                // document.getElementById('address-input').value = null;
                $('#address-input').val('');
                isExceptionForced = false;
                $('#result-box').fadeIn('fast');
                document.getElementById('address-input').style.height = '';
            },
            error: function (error) {
                // console.error('Error:', error);
                $('#result-box').fadeIn('fast');
            }
        });
    });
    var exceptionBtnClicked = false;
    $('#exception-checkbox').change(function () {
        // Check if the checkbox is checked
        if ($(this).is(':checked')) {
            // Enable the button if checkbox is checked
            $('#exception-btn').prop('disabled', false);
            $('#exception-checkbox').css('border-color', '');
        } else {
            if (exceptionBtnClicked) {
                $('#exception-checkbox').css('border-color', 'red');
                // console.log("Color higlighted!")
            } else {
                $('#exception-checkbox').css('border-color', '');
            }
            $('#exception-btn').prop('disabled', true);
        }
    });


    // Event listener for the exception button
    $('#exception-btn').on('click', function () {
        exceptionBtnClicked = true;
        // var forceException = $('#exception-checkbox').is(':checked');
        if (isExceptionForced) {
            alert("Exception already created for this address and sent to Database!");
            $('#exception-checkbox').prop('checked', false);
            $('#exception-btn').prop('disabled', true);
        } else {
            // var unsatisfied_address = $('#address-input').val();

            // AJAX request to the server with the forceException value
            $.ajax({
                type: "POST",
                url: "/forceException",
                data: { address: unsatisfied_address },
                success: function (response) {
                    // console.log("Response:", response);
                    isExceptionForced = true;
                    alert("Forced Exception is Created! \nException Dictionary is sent to the Database!");
                    $('#exception-checkbox').prop('checked', false);
                    $('#exception-btn').prop('disabled', true);

                    // if (response.download_url) {
                    //     // Create a temporary link element
                    //     var tempLink = document.createElement('a');
                    //     tempLink.href = response.download_url;
                    //     tempLink.download = 'Forced_Except.json';
                    //     document.body.appendChild(tempLink);
                    //     tempLink.click();
                    //     document.body.removeChild(tempLink);
                    // }
                },
                error: function (error) {
                    console.error("Error:", error);
                }
            });
        }
    });
    var parsedByInfo = $('<div id="parsed-by-info" style="display: flex; justify-content: center; align-items: center; margin-top: 10px;"></div>');
    $('#exception-controls').after(parsedByInfo);


    function renderAddressData(data) {
        var tableBody = $('#resultBody');
        tableBody.empty();
        var tableHTML = '';

        if (data && data.result && data.result.Output && data.result.Parsed_By) {
            var addressComponents = data.result.Output;
            // console.log("Data: ", data);
            var parsedBy = data.result.Parsed_By;
            $('#parsed-by-info').text('Address Parsed By : ' + parsedBy);
            addressComponents.forEach(function (array, index) {

                tableHTML += '<tr>';
                tableHTML += '<td>' + array[2] + '</td>'; // Token
                tableHTML += '<td>' + array[0] + '</td>'; // Address
                tableHTML += '<td>' + array[1] + '</td>'; // Component
                tableHTML += '<td>' + array[3] + '</td>'; // Component Description
                //tableHTML += '<td>' + parsedBy + '</td>'; // Parsed By
                // if (index === 0) {
                //     tableHTML += '<td rowspan="' + addressComponents.length + '" style="vertical-align: top;">' + parsedBy + '</td>';
                // }
                tableHTML += '</tr>';
                // tableHTML += wrapTableData(array);
            });

            tableBody.html(tableHTML);
            $('#resultTable').show();
            $('#resultTable thead').show();
        } else {
            // console.error('Data structure issue: Output is undefined.');
            $('#resultTable').hide();
            $('#resultTable thead').hide();
        }
    }
});

//------------------------------------------------------------------------------------------------------------
//                     Batch Parser Tab Functionality
//------------------------------------------------------------------------------------------------------------

$('#BF').click(function () {
    location.reload(true);
    document.getElementById("BF").value = ' active';
});



$(document).ready(function () {
    $('#metrics-display').hide();
    $('#file-upload').on('change', function () {
        $('#metrics-display').hide();
    });
    $('#upload-btn').css('width', '170px')
    $('#file-upload').css('width', '250px')




    $('#BatchForm').submit(function (event) {

        document.getElementById('spinner').style.display = 'block';
        event.preventDefault();
        var fileData = new FormData(this);
        $.ajax({
            url: '/Batch_Parser',
            type: 'POST',
            data: fileData,
            contentType: false,
            processData: false,
            xhr: function () {
                var xhr = new window.XMLHttpRequest();
                // Event listener for the upload progress
                xhr.upload.addEventListener('progress', function (e) {
                    if (e.lengthComputable) {
                        var percentComplete = Math.round((e.loaded / e.total) * 100);
                        // Update the progress bar with the new percentage
                        $('#progressBar').val(percentComplete);
                        $('#progressBarText').text(percentComplete + '%'); // If you also have a text element to update
                    }
                }, false);
                return xhr;
            },
            success: function (response) {

                // This code is called when the AJAX request completes successfully
                // console.log('File uploaded!');
                $('#progressBar').val(50); // Make sure the progress bar shows 100% at the end
                $('#progressBarText').text('100%'); // Update the text to 100% when upload completes
                // Further actions based on response
                if (response.status_check_url) {
                    // console.log(response.status_check_url)
                    document.getElementById('spinner').style.display = 'block';
                    //pollForMetrics(response.status_check_url, response.download_url);
                    setTimeout(function () { pollForMetrics(response.status_check_url, response.download_url); }, 500);
                }
            },
            error: function (error) {
                // This code is called if the AJAX request fails
                // console.log('Upload error:', error);
                document.getElementById('spinner').style.display = 'none';
                alert("Unexpected Error: Please ensure the file is pipe delimited!");
            }
        });
    });


});

function pollForMetrics(statusCheckUrl, downloadUrl) {
    $.ajax({
        url: statusCheckUrl,
        type: 'GET',
        success: function (response) {

            if (response.metrics) {
                // document.getElementById('spinner').style.display = 'none';
                var formattedText = response.metrics.metrics.replace(/\n/g, '<br>').replace(/\t/g, '&nbsp;&nbsp;&nbsp;&nbsp;');

                $('#metrics-display').html(formattedText).show();

                // console.log(downloadUrl)
                triggerDownload(downloadUrl, response.output_file_path);
            } else {
                setTimeout(function () { pollForMetrics(statusCheckUrl, downloadUrl); }, 3000);
            }
            $('#file-upload').val('');
        },
        error: function (error) {
            // document.getElementById('spinner').style.display = 'none';
            // console.log('Error:', error);
            // alert("Error fetching metrics");
        }
    });
}

function triggerDownload(downloadUrl, removeURL) {
    var tempLink = document.createElement('a');
    tempLink.href = downloadUrl;
    tempLink.download = 'output.zip'; // Or the appropriate filename
    document.body.appendChild(tempLink);
    tempLink.click();
    document.body.removeChild(tempLink);

    // Set a delay before calling removeFile
    setTimeout(function () {
        removeFile(removeURL);
    }, 3000);
}



function removeFile(output_file_path) {
    // console.log("output_file_path", output_file_path);
    $.ajax({
        url: "/removefile",
        type: 'POST',
        contentType: 'application/json',  // Set the content type to application/json
        data: JSON.stringify({ output_file_path: output_file_path }),
        success: function (response) {
            document.getElementById('spinner').style.display = 'none';
            // console.log(response)
            // location.reload();
        },
        error: function (error) {
            document.getElementById('spinner').style.display = 'none';
            console.error('Error:', error); // Log the error for debugging
            alert("Error removing the file");
        }
    });
}


//------------------------------------------------------------------------------------------------------------
//                                  Map Creation Form Tab Functionality
//------------------------------------------------------------------------------------------------------------      

let data = [];
let currentKeyIndex = 0;
let initialDataLength = 0;
let dicIndex = 0;
let total_dict;

$('#MCF').click(function () {
    location.reload(true);

    // updateUserDropdown();
    // updateTimestampDropdown();
    // document.getElementById("MCF").value = ' active';
});

// function reloadPageAndSetActive() {
//     location.reload(true);
//     document.getElementById("MCF").value = ' active';
// }

// $(document).ready(function() {
//     $('#MCF').click(reloadPageAndSetActive);
// });

document.addEventListener('DOMContentLoaded', function () {
    const runDropdown = document.getElementById('run-dropdown');
    const userDropdown = document.getElementById('user-dropdown');
    const timestampDropdown = document.getElementById('timestamp-dropdown');

    if (runDropdown) {
        runDropdown.addEventListener('change', updateUserDropdown);
    }

    if (userDropdown) {
        userDropdown.addEventListener('change', updateTimestampDropdown);
    }

    // Your fetch for initial load of Run dropdown
});
document.getElementById('run-dropdown').addEventListener('change', function () {
    document.getElementById("region").value = "";
    document.getElementById("AddressType").value = "";
    document.getElementById("Approved?").value = "";
    document.getElementById("comment").value = "";
    document.getElementById("region").style.border = "2px solid #2e4c8c";
    document.getElementById("AddressType").style.border = "2px solid #2e4c8c";
    document.getElementById("Approved?").style.border = "2px solid #2e4c8c";
    document.getElementById("comment").style.border = "2px solid #2e4c8c";
})



// Function to update User dropdown based on Run selection
function updateUserDropdown() {
    const run = document.getElementById('run-dropdown').value;
    fetch('/get_users/' + run)
        .then(response => response.json())
        .then(data => {
            const userDropdown = document.getElementById('user-dropdown');
            // Add default option at the start
            userDropdown.innerHTML = `<option>Select User</option>` + data.map(user => `<option>${user}</option>`).join('');
        });
}


// Function to update Timestamp dropdown based on User selection
function updateTimestampDropdown() {
    const run = document.getElementById('run-dropdown').value;
    const user = document.getElementById('user-dropdown').value;
    // console.log("admin: ", user);
    fetch('/get_timestamps/' + run + '/' + user)
        .then(response => response.json())
        .then(data => {
            const timestampDropdown = document.getElementById('timestamp-dropdown');
            timestampDropdown.innerHTML = data.map(timestamp => `<option>${timestamp}</option>`).join('');
        });

}

// Initial load of Run dropdown
window.onload = function () {
    fetch('/get_runs')
        .then(response => response.json())
        .then(data => {
            const runDropdown = document.getElementById('run-dropdown');
            runDropdown.innerHTML += data.map(run => `<option>${run}</option>`).join('');
        });
};

document.getElementById("loadFileBtn").addEventListener("click", function () {
    // const jsonFileInput = document.getElementById("jsonFileInput");

    // if (jsonFileInput.files.length === 0) {
    //     alert("Please select a JSON file.");
    //     return; // Stop the function if no file is selected
    // }
    document.getElementById('spinners').style.display = 'block';
    const run = document.getElementById('run-dropdown').value;
    const user = document.getElementById('user-dropdown').value;
    const timestamp = document.getElementById('timestamp-dropdown').value;
    // console.log("LoadFileBtn Clicked")
    $.ajax({
        url: '/process_dropdown_data', // Update this URL to your Flask endpoint
        type: 'POST',
        contentType: 'application/json',
        data: JSON.stringify({
            run: run,
            user: user,
            timestamp: timestamp
        }),
        success: function (response) {
            // console.log('Success:', response);
            // console.log("Response: ", response);
            total_dict = response.total_dict;
            // if (response.total_dict !== undefined) {
            //     console.log("Total Dict: ", response.total_dict);
            //     // You can now use response.total_dict as needed
            // }

            // document.getElementById('spinners').style.display = 'block';
            loadFile(response.data, total_dict); // Call loadFile() on success
            document.getElementById('spinners').style.display = 'none';

        },
        error: function (error) {
            console.error('Error:', error);
        }
    });
});
document.getElementById("clear&exitBtn").addEventListener("click", exitFunction);


function loadFile(received_data, total_dict) {
    data = [];
    currentKeyIndex = 0;
    initialDataLength = 0;
    dicIndex = 0;
    resetUIElements();
    // console.log('Data Received in LoadFile() :',received_data);
    // console.log("Total Dictionaries: ", total_dict);
    let container = document.getElementById("mapdata");
    if (container) {
        container.style.display = 'grid';
    } else {
        console.error("Container element not found");
        return; // Exit the function if the container is not found
    }



    try {
        const newData = received_data;
        data = newData; // Append new data to existing data array
        initialDataLength = data.length;
        if (data.length > 1) {
            document.getElementById("submit&NextBtn").style.display = 'block';
            document.getElementById("submitBtn").style.display = 'none';
        } else {
            document.getElementById("submit&NextBtn").style.display = 'none';
            document.getElementById("submitBtn").style.display = 'block';
        }
        currentKeyIndex = 0;
        showNext(total_dict); // Start processing data if it's the first file

    } catch (error) {
        // console.error("Parsing error:", error);
        exitFunction();
        alert("Invalid JSON data. Please provide a valid JSON file.", error.message);
    }

}

function showNext(total_dict) {
    // console.log("ShowNext total_dict Received: ", total_dict)

    if (currentKeyIndex < data.length) {
        const currentData = data[currentKeyIndex];
        const keys = Object.keys(currentData);
        console.log(keys)
        // console.log("Keys : ",keys)
        const filenameElement = document.getElementById("filename");
        filenameElement.value = document.getElementById('timestamp-dropdown').value;
        document.getElementById("recordId").value = currentData["Record ID"];
        document.getElementById("inputValue").value = currentData["INPUT"];
        let filteredData = keys.filter(element => element !== "Record ID" && element !== "INPUT");

        document.getElementById("mask-inputValue").value = filteredData;

        const otherDataKey = findThirdObjectKey(currentData);
        const otherData = currentData[otherDataKey];
        const tableBody = document.getElementById("table-body");
        tableBody.innerHTML = "";
        fetchOptionsAndPopulateDropdowns(otherData, otherDataKey, total_dict);
        if (data.length > 1) {
            document.getElementById("submit&NextBtn").style.display = 'block';
            document.getElementById("submitBtn").style.display = 'none';
        } else {
            document.getElementById("submit&NextBtn").style.display = 'none';
            document.getElementById("submitBtn").style.display = 'block';
        }
        updateDictionaryDisplay();
    } else {
        alert("End of data reached");
        currentKeyIndex = data.length;
        exitFunction();
    }

}



function updateDictionaryDisplay() {
    const totalDictionaries = initialDataLength;
    const TotalDictionaries = total_dict;
    // const keyIndex = currentKeyIndex;
    // console.log("update Dictionaries total dict received: ",TotalDictionaries);
    // console.log("update Dictionaries total dict received: ",total_dict);
    const currentDictionaryIndex = totalDictionaries > 0 ? dicIndex + 1 : 0;
    document.getElementById("currentDictionaryDisplay").textContent = `${currentDictionaryIndex}/${totalDictionaries}`;
    document.getElementById("totaldictionaryDisplay").textContent = `${TotalDictionaries}`;
}

function fetchOptionsAndPopulateDropdowns(otherData, otherDataKey) {
    fetch("/AddressComponents_dropdown")
        .then(response => {
            if (!response.ok) {
                throw new Error('Network response was not ok');
            }
            return response.json();
        })
        .then(options => {
            populateDropdowns(otherData, otherDataKey, options);
        })
        .catch(error => console.error('Error fetching options:', error));
}

function handleNextDictionary(index) {
    // console.log(index);
    // console.log("HandleNext totaldict: ", total_dict);
    // console.log("Initial Data Length Before removal: ", data.length)
    if (index >= 0 && index < data.length) {
        data.splice(index, 1); // Remove the processed dictionary
        // console.log("data splice: ",data.splice(index,1));
        // Update the dictionary display
        updateDictionaryDisplay(total_dict);

        if (index < data.length) {
            dicIndex++;
            showNext(total_dict);
            // console.log("data length for show next :",data.length);
            // Do not automatically call showNext here. It will be called after user actions.
        }
        else if (total_dict <= 50 && total_dict > 1) {
            alert("All " + total_dict + " dictionaries processed!");
            exitFunction();
        } else if (total_dict > 50) {
            alert("50 out of " + total_dict + " dictionaries processed!");
            exitFunction();
        } else {
            alert("Successfully processed!");
            exitFunction();
        }
    }
}

function populateDropdowns(otherData, otherDataKey, options) {
    const tableBody = document.getElementById("table-body");
    // console.log("populateDropdowns called with data", otherData);

    if (otherDataKey) {
        otherData.forEach(function (item) {
            const row = document.createElement("tr");
            tableBody.appendChild(row);
            const reorderedItem = [item[2], item[0], item[3]];
            reorderedItem.forEach(function (value, index) {
                //const value = item[index];
                const td = document.createElement("td");
                if (index == 2) {
                    // Convert value column to a dropdown menu
                    const valueSelect = document.createElement("select");
                    valueSelect.name = "value";
                    options.forEach(function (optionValue) {
                        if (optionValue == "Not Selected") {
                            const optionElement = document.createElement("option");
                            optionElement.value = optionValue;
                            optionElement.disabled = true;
                            optionElement.textContent = optionValue;
                            valueSelect.appendChild(optionElement);
                        }

                        else {
                            const optionElement = document.createElement("option");

                            optionElement.value = optionValue;
                            optionElement.textContent = optionValue;
                            valueSelect.appendChild(optionElement);
                        }
                    });
                    valueSelect.value = value;
                    td.appendChild(valueSelect);
                } else {
                    td.textContent = value;
                }
                row.appendChild(td);
            });
        });
        // currentKeyIndex++;
        // dicIndex++;

    } else {
        alert("No third object found in the current data.");
    }
    // console.log("Finished populating table");
}

function exitFunction() {
    // Hide the container instead of removing it
    const container = document.getElementById("mapdata");
    if (container) {
        container.style.display = "none"; // Hide the container
    }
    location.reload(true);


    // Reset the file input
    const fileInput = document.getElementById("jsonFileInput");
    if (fileInput) {
        fileInput.value = "";
    }
}

function findThirdObjectKey(data) {
    for (const key in data) {
        if (key !== "Record ID" && key !== "INPUT") {
            return key;
        }
    }
    return null; // No third object key found
}


//-----------------------------------------------------------------------------
//                      "Collect and Push Map Creation Form data to Server"
//-----------------------------------------------------------------------------

function collectData() {
    // Example: Collect data from input fields
    // console.log("collectData called");
    const record_Id = document.getElementById("recordId").value;
    const timestamp = document.getElementById('timestamp-dropdown').value;
    const input_Value = document.getElementById("inputValue").value;
    const mask = document.getElementById("mask-inputValue").value;
    const comment = document.getElementById("comment").value;
    const address_region = document.getElementById("region") ? document.getElementById("region").value : null;
    const Address_Type = document.getElementById("AddressType") ? document.getElementById("AddressType").value : null;
    const approval = document.getElementById("Approved?") ? document.getElementById("Approved?").value : null;
    // const approved_by = document.getElementById("approvedby") ? document.getElementById("approvedby").value : null;
    const user = document.getElementById('user-dropdown').value;


    // Example: Collect data from table
    const mappingData = [];
    const tbody = document.getElementById("table-body");
    if (!tbody) {
        // console.error("Tbody element with ID 'table-body' not found.");
        return; // Exit the function  the tbody isn't found
    }
    const rows = tbody.getElementsByTagName("tr");
    // console.log("Rows: ", rows);
    const dicData = {};
    for (let i = 0; i < rows.length; i++) {
        const cells = rows[i].getElementsByTagName("td");
        // console.log(`Row ${i} cells:`, cells);
        if (cells.length < 3) {
            console.error(`Row ${i} is missing one or more cells.`);
            continue;
        }
        const rowData = {
            "Mask Token": cells[0].textContent,
            "Address Token": cells[1].textContent,
        };
        rowData["Address Component"] = cells[2].querySelector("select")
            ? cells[2].querySelector("select").value
            : cells[2].textContent;
        dicData[i + 1] = rowData["Address Component"]
        mappingData.push(rowData);
    }
    // console.log("Data collected", mappingData);
    result = {
        "Record Id": record_Id,
        "Timetamp": timestamp,
        "Input": input_Value,
        "Mask Pattern": mask,
        "Region": address_region,
        "Type": Address_Type,
        "Address Approved?": approval,
        "Comment": comment,
        "User": user,
        "Mapping Data": mappingData
    };
    result[mask] = dicData;
    return result;

}


document.getElementById("submitBtn").addEventListener("click", function () {
    // setTimeout(() => {
    // }, 100);
    const data = collectData();
    submitButtonHandler(data, currentKeyIndex);
});

document.getElementById("submit&NextBtn").addEventListener("click", function () {
    const data = collectData();
    submitButtonHandler(data, currentKeyIndex);
});

function submitButtonHandler(collectedData, index) {
    const approved = document.getElementById("Approved?").value;
    const isDropdownsValid = validateDropdowns();
    const commentValidation = document.getElementById("comment").value;
    const commentField = document.getElementById("comment");
    const approvedField = document.getElementById("Approved?");
    
    document.getElementById("region").addEventListener('change', function () {
        this.style.border = "2px solid #2e4c8c";
        this.style.transition = "none";
    });
    document.getElementById("AddressType").addEventListener('change', function () {
        this.style.border = "2px solid #2e4c8c";
        this.style.transition = "none";
    });
    commentField.addEventListener('change', function () {
        this.style.border = "2px solid #2e4c8c";
        this.style.transition = "none";
    });
    approvedField.addEventListener('change', function () {
        this.style.border = "2px solid #2e4c8c";
        this.style.transition = "none";
        // Remove highlight from comment if Approved? is changed to Yes
        if (this.value === "Yes") {
            commentField.style.border = "2px solid #2e4c8c";
        }
    });

    if (approved === "Yes") {
        if (isDropdownsValid) {
            checkForExistingMask(collectedData["Mask Pattern"], collectedData, index);
        } else {
            if (document.getElementById("region").value === "") {
                document.getElementById("region").style.border = "3px solid #FF1F1F";
            }
            if (document.getElementById("AddressType").value === "") {
                document.getElementById("AddressType").style.border = "3px solid #FF1F1F";
            }
            alert("Please Fill all Required Fields!");
        }
    } else if (approved === "No" || approved === null) {
        // Highlight comment field if Approved? is No or null
        commentField.style.border = "3px solid #FF1F1F";

        if (isDropdownsValid && commentValidation !== "") {
            sendDataToServer(collectedData, index);
        } else {
            if (document.getElementById("region").value === "") {
                document.getElementById("region").style.border = "3px solid #FF1F1F";
            }
            if (document.getElementById("AddressType").value === "") {
                document.getElementById("AddressType").style.border = "3px solid #FF1F1F";
                return alert("Address Type is not selected");
            }
            if (commentField.value === "") {
                commentField.style.border = "3px solid #FF1F1F";
                return alert("Please provide the comment for the Rejection");
            }
            alert("Please fill in all required fields!");
        }
    } else {
        approvedField.style.border = "3px solid #FF1F1F";
        alert("Please fill in all required fields!");
        if (document.getElementById("region").value === "") {
            document.getElementById("region").style.border = "3px solid #FF1F1F";
        }
        if (document.getElementById("AddressType").value === "") {
            document.getElementById("AddressType").style.border = "3px solid #FF1F1F";
        }
        if (commentField.value === "") {
            commentField.style.border = "3px solid #FF1F1F";
        }
    }
}





function validateDropdowns() {
    // Helper function to check if element exists and its value is not empty
    function isDropdownValid(id) {
        const element = document.getElementById(id);
        const isValid = element.value !== "" && element.value !== "Not Selected";
        // console.log(`Dropdown ${id}: Value = ${element.value}, IsValid = ${isValid}`);
        // document.getElementById("id").style.border="4px solid #FF1F1F";
        let isValidComponents = true;
        const dropdowns = document.querySelectorAll("select[name='value']");
        // console.log(dropdowns);

        for (let i = 0; i < dropdowns.length; i++) {
            // console.log("asdasfafda");
            if (dropdowns[i].value === "Not Selected") {
                dropdowns[i].style.border = "3px solid #FF1F1F";
                // console.log(`Address Component Dropdown ${i}: Value = Not Selected`);
                isValidComponents = false;
            }
            dropdowns[i].addEventListener('change', function () {
                dropdowns[i].style.border = "2px solid #2e4c8c";
            })
            // console.log("isValidComponents :", isValidComponents)
        }
        // console.log("isValid : ", isValid)
        return isValid && isValidComponents;
    }

    if (!isDropdownValid("region") ||
        !isDropdownValid("AddressType") ||
        !isDropdownValid("Approved?")) {
        return false;
    }

    // Check each address component dropdown
    return true;
}



function checkForExistingMask(mask, data, index) {
    $.ajax({
        type: "POST",
        url: "/check-mask-existence",
        contentType: "application/json",
        data: JSON.stringify({ mask: mask }),
        success: function (response) {
            if (response.exists) {
                if (confirm("Mask found in KnowledgeBase, do you want to overwrite the existing entry?")) {
                    sendDataToServer(data, index);
                } else {
                    resetUIElements();
                    handleNextDictionary(index);

                }
            } else {
                sendDataToServer(data, index);
            }
        },
        error: function (xhr, status, error) {
            console.error("Error:", error);
            // Handle errors
        }
    });
}



function sendDataToServer(data, index) {
    console.log("Send Data Server Index: ", index);
    $.ajax({
        type: "POST",
        url: "/MapCreationForm-Data",
        contentType: "application/json",
        data: JSON.stringify(data),
        success: function (response) {
            // console.log("Success:", response);
            if (data["Address Approved?"] === "Yes") {
                alert("Mapping Created and sent to Knowledgebase");
            }
            resetUIElements();
            handleNextDictionary(index);
        },
        error: function (xhr, status, error) {
            console.error("AJAX Error: Status -", status, "Error -", error);
        }
    });
}

function resetUIElements() {
    // Example: Reset input fields
    // document.getElementById("recordId").value = "";
    // document.getElementById("inputValue").value = "";
    // document.getElementById("mask-inputValue").value = "";
    document.getElementById("comment").value = "";

    // Reset dropdowns if needed
    resetDropdown("region");
    resetDropdown("AddressType");
    resetDropdown("Approved?");
    resetDropdown("approvedby");

    // Hide or reset other UI elements as needed
    document.getElementById("submit&NextBtn").style.display = 'none';
    document.getElementById("submitBtn").style.display = 'block';

    // Clear any dynamic content (e.g., table rows)
    const tableBody = document.getElementById("table-body");
    tableBody.innerHTML = "";
}

function resetDropdown(dropdownId) {
    const dropdown = document.getElementById(dropdownId);
    if (dropdown) {
        dropdown.value = ""; // or the default value
    }
}





//------------------------------------------------------------------------------------------------------------
//                                  User Defined Components Tab Functionality
//------------------------------------------------------------------------------------------------------------
$('document').ready(function () {
    $('#fetchall').click();
    $('#fetchall').hide();
});

$('#userdfc').click(function () {
    location.reload(true);
    document.getElementById("userdfc").value = ' active';
});

function fetchComponentData() {
    $.ajax({
        type: "POST",
        url: "/UserDefinedComponents",
        success: function (response) {
            // console.log('Response:', response);
            populateComponentsTable(response.result);
        },
        error: function (error) {
            console.error('Error:', error);
        }
    });
}
$('#userdfc').click(function () {
    location.reload();
    $('#fetchall').click();
    $('#fetchall').hide();
    document.getElementById("BF").value = ' active';
    
    // location.reload(true);
});

function toTitleCase(str) {
    return str.replace(/\w\S*/g, function (txt) {
        return txt.charAt(0).toUpperCase() + txt.substr(1).toLowerCase();
    });
}
function populateComponentsTable(components) {
    var tableBody = $('#udresultBody');
    tableBody.empty(); // Clear existing table rows
    var tableHTML = '';
    
    // Loop through the received JSON data
    for (var key in components) {
        tableHTML += '<tr>';
        tableHTML += '<td>' + key + '</td>'; // Component
        tableHTML += '<td>' + components[key] + '</td>'; // Component Description
        tableHTML += '<td class="actions">';
        tableHTML += '<button class="edit-btn" onclick="editRow(this,event)">Edit</button>';
        tableHTML += '<button class="delete-btn" onclick="deleteRow(this,event)">Delete</button>';
        tableHTML += '</td>';
        tableHTML += '</tr>';
    }

    tableBody.html(tableHTML); // Inject the generated HTML into the table body
    $('#udresultTable').show(); // Show the table
    $('#udresultTable thead').show(); // Show the table header
}
$('#udresultBody').on('click', 'td', function (event) {
    event.stopPropagation();
});


function addComponent() {
    // Create a new row for adding a component
    var newRow = '<tr class="new-row">' +
        '<td><input type="text" class="editable" placeholder="New Component" maxlength="8"></td>' +
        '<td><input type="text" class="editable" placeholder="New Description" maxlength="24"></td>' +
        '<td class="actions">' +
        '<button class="save-btn" id="save-btn" onclick="saveNewRow(this,event)">Save</button>' +
        '<button class="cancel-btn" id="cancel-btn" onclick="cancelAddRow(this,event)">Cancel</button>' +
        '</td>' +
        '</tr>';

    // Append the new row at the end of the table
    $('#udresultBody').append(newRow);
    const editButtons = document.querySelectorAll('.edit-btn');
    const delButtons = document.querySelectorAll('.delete-btn');
    // Loop through the NodeList and disable each button
    document.getElementById("addComponent").disabled = true;
    editButtons.forEach(function (button) {
        button.disabled = true;
    });
    delButtons.forEach(function (button) {
        button.disabled = true;
    });
}

function saveNewRow(button,event) {
    event.stopPropagation();
    var newRow = $(button).closest('tr.new-row');
    var cells = newRow.find('td:not(.actions)');
    var Component = cells.eq(0).find('input').val();
    
    var Description = cells.eq(1).find('input').val();
    var newComponent = Component.toUpperCase();
    var newDescription = toTitleCase(Description);

    if (!newComponent.startsWith("USAD_")) {
        alert('The component Value must start with "USAD_". Please update the value accordingly.');
        location.reload();
        return;
    }
    // Perform validation if needed
    if (newComponent && newDescription) {
        // Send the new data to the server
        $.ajax({
            type: "POST",
            url: "/add_new_component",
            data: { newComponent: newComponent, newDescription: newDescription },
            success: function (response) {
                if (response.result.error) {
                    // If the server returns an error, show an alert
                    alert(response.result.error);
                } else {
                    // Proceed with successful addition
                    alert("New Component: "+ newComponent +" & new Description: "+newDescription+" is Added Successfully!");
                    location.reload();
                    // newRow.remove();
                    // fetchComponentData();
                }
            },
            error: function (error) {
                console.error('Error adding new record:', error);
                // Handle error if needed
            }
        });
    } else {
        alert('Please provide both component and description values.');
        // Handle the case where either component or description is not provided
    }
    
}


function cancelAddRow(button) {
    // Remove the new row upon cancellation
    var newRow = $(button).closest('tr.new-row');
    document.getElementById("addComponent").disabled = false;
    newRow.remove();
    document.getElementById('addClueComponent').disabled = false;
}


var oldComponent, oldDescription;
function editRow(button, event) {

    var row = $(button).closest("tr");
    var cells = row.find("td:not(.actions)");
    event.stopPropagation();
    event.preventDefault();
    cells.each(function (index) {
        var cell = $(this);
        var cellText = cell.text();
        cell.attr('data-old-value', cellText);
        var maxLength = index === 0 ? 8 : 24; // 8 for the first cell, 20 for the second
        cell.html('<input type="text" class="editable" maxlength="' + maxLength + '" value="' + cellText + '">');
    });
    oldComponent = row.find('td:eq(0)').data('old-value');
    oldDescription = row.find('td:eq(1)').data('old-value');

    // Log values to console for debugging
    // console.log('oldComponent:', oldComponent);
    // console.log('oldDescription:', oldDescription);

    var actionsCell = row.find('.actions');
    actionsCell.html('<button class="save-btn" onclick="saveRow(this,event)">Save</button>' +
        '<button class="cancel-btn" onclick="cancelEditRow(this,event)">Cancel</button>');
    enableButtons(row);
    const editButtons = document.querySelectorAll('.edit-btn');
    const delButtons = document.querySelectorAll('.delete-btn');

    // Loop through the NodeList and disable each button
    editButtons.forEach(function (button) {
        button.disabled = true;
    });
    delButtons.forEach(function (button) {
        button.disabled = true;
    });
    document.getElementById('addComponent').disabled = true;
}


// Function to cancel editing a row
function cancelEditRow(button) {
    var row = $(button).closest('tr');
    var cells = row.find('td:not(.actions)');

    cells.each(function () {
        var cell = $(this);
        var oldValue = cell.attr('data-old-value');
        cell.text(oldValue);
    });

    var actionsCell = row.find('.actions');
    actionsCell.html('<button class="edit-btn" onclick="editRow(this,event)">Edit</button>' +
        '<button class="delete-btn" onclick="deleteRow(this,event)" disabled>Delete</button>');
    enableButtons(row);

    // Preserve oldComponent and oldDescription
    oldComponent = cells.eq(0).text();
    oldDescription = cells.eq(1).text();
    const editButtons = document.querySelectorAll('.edit-btn');
    const delButtons = document.querySelectorAll('.delete-btn');
    document.getElementById('addComponent').removeAttribute('disabled');
    // Loop through the NodeList and disable each button
    editButtons.forEach(function (button) {
        button.removeAttribute('disabled');
    });
    delButtons.forEach(function (button) {
        button.removeAttribute('disabled');
    });
}


// Function to handle the save button click event
function saveRow(button,event) {
    event.stopPropagation(); 
    var row = $(button).closest('tr');
    var cells = row.find('td:not(.actions)');
    var newData = []; // Array to hold modified data

    cells.each(function () {
        var cell = $(this);
        var input = cell.find('input');
        var newValue = input.val();
        cell.text(newValue); // Update cell with new value

        // Store new data in an array
        newData.push(newValue);
    });
    if (!newData[0].startsWith("USAD_")) {
        alert('The component must start with "USAD_". Please update the value accordingly.');
        location.reload();
        return;  // Stop execution if the validation fails
    }
    // cells.each(function (index) {
    //     var cell = $(this);
    //     cell.text(newData[index]); // Update cell with new value
    // });

    var payload = {
        oldComponent: oldComponent,
        oldDescription: oldDescription,
        newComponent: newData[0].toUpperCase(), // Updated component value after editing
        newDescription: toTitleCase(newData[1])
    };

    var actionsCell = row.find('.actions');
    actionsCell.html('<button class="edit-btn">Edit</button>' +
        '<button class="delete-btn" disabled>Delete</button>');
    enableButtons(row);
    // Preserve oldComponent and oldDescription
    oldComponent = cells.eq(0).text();
    oldDescription = cells.eq(1).text();

    // Send the modified data to the server
    saveChangesToServer([payload],event); // Wrap payload in an array

    // Reset oldComponent and oldDescription
    //oldComponent = null;
    //oldDescription = null;
}

function saveChangesToServer(payload, event) {
    // alert('Data sent to server:', payload);
    var payload_component = payload[0];
    var oldComponent = payload_component.oldComponent;
    var newComponent = payload_component.newComponent;
    var oldDescription = payload_component.oldDescription;
    var newDescription = payload_component.newDescription;

    if (oldComponent == newComponent && oldDescription== newDescription) {
        alert("No Changes Detected!")
        location.reload()
        return;
    } else {
        
        $.ajax({
            type: "GET",
            url: "/get_mask_count",
            data: { component: oldComponent },
            success: function (response) {
                // Check if response has the 'result' property
                if ('result' in response && 'maskCount' in response.result) {
                    var confirmationMessage = "Are you sure you want to Edit this record?\n";
                    confirmationMessage += "Number of Masks and Dictionaries Edited: " + response.result.maskCount;
                    
                    // Use a JavaScript confirm dialog
                    var confirmation = window.confirm(confirmationMessage);
                    if (!confirmation) {
                        location.reload()
                    }
                    else {
                        console.log('Attempting to show spinner...');
                        document.getElementById('uspinner').style.display = 'block';
                        console.log('Spinner visibility should now be set to block.');
                        $.ajax({
                            type: "POST",
                            url: "/save_changes",
                            contentType: "application/json",
                            data: JSON.stringify({ components: payload }), // Wrap payload in an object
                            success: function (response) {
                                if (response.result.error){
                                    alert(response.result.error)
                                    document.getElementById('uspinner').style.display = 'none';
                                } else
                                {
                                    alert(response.result.message)
                                    document.getElementById('uspinner').style.display = 'none';
                                    location.reload();// reload when the save button is clicked
                                }
                            },
                            error: function (error) {
                                console.error('Error Editing record:', error);
                                // Handle error
                            }
                        })
                            .always(function () {
                                resetButtons();
                            });
                    }
                }
                else {
                    console.error('Invalid response format:', response);
                }
            }, error: function (error) {
                console.error('Error getting mask count:', error);
            }
        });

    }

    event.preventDefault(); // Prevent the default action of the button
    event.stopPropagation(); // Stop the event from propagating further
}

function deleteRow(button, event) {
    var row = $(button).closest("tr");
    var component = row.find('td:first').text(); // Assuming the Component column is the first one

    // Send a GET request to the server to get the count of associated masks
    $.ajax({
        type: "GET",
        url: "/get_mask_count",
        data: { component: component },
        success: function (response) {
            // Check if response has the 'result' property
            if ('result' in response && 'maskCount' in response.result) {
                var confirmationMessage = "Are you sure you want to delete this record?\n";
                confirmationMessage += "Number of Masks and Dictionaries affected: " + response.result.maskCount;

                // Use a JavaScript confirm dialog
                var confirmation = window.confirm(confirmationMessage);
                if (confirmation) {
                    // Send a POST request to the server for deletion
                    $.ajax({
                        type: "POST",
                        url: "/delete_record",
                        data: { component: component }, // Send the component to be deleted
                        success: function (response) {
                            row.remove(); // Remove the row from the table upon successful deletion

                            // Show alert with the component name
                            alert("The component '" + component + "' has been successfully deleted.");

                            location.reload();
                        },
                        error: function (error) {
                            console.error(error);
                        }
                    });
                } else {
                    // Do nothing if the user selects "Cancel" in the confirmation dialog
                }
            } else {
                console.error('Invalid response format:', response);
            }
        },
        error: function (error) {
            console.error('Error getting mask count:', error);
        }
    });

    event.preventDefault(); // Prevent the default action of the button
    event.stopPropagation(); // Stop the event from propagating further
}




function resetButtons() {
    // Implement the logic to reset the buttons as needed
    // For example, enable the edit and delete buttons
    // Replace this with your actual logic
    $('.edit-btn').prop('disabled', false);
    $('.delete-btn').prop('disabled', false);
    // Reattach the click event for the edit button
    $('.edit-btn').off('click').on('click', function (event) {
        editRow(this, event);
    });
    $('.delete-btn').off('click').on('click', function (event) {
        deleteRow(this, event);
    });
}

function downloadLogs() {
    $.ajax({
        url: '/download/logs',
        method: 'GET',
        xhrFields: {
            responseType: 'blob'
        },
        success: function (data) {
            var downloadUrl = URL.createObjectURL(data);
            var a = document.createElement('a');
            a.href = downloadUrl;
            a.download = 'logs.txt';
            document.body.appendChild(a);
            a.click();
            document.body.removeChild(a);
            URL.revokeObjectURL(downloadUrl);
        },
        error: function (jqXHR, textStatus, errorThrown) {
            // console.log('AJAX call failed: ', textStatus, errorThrown);
            alert('Failed to download the logs file.');
        }
    });
}



function enableButtons(row) {
    row.find('.edit-btn').prop('disabled', false);
    row.find('.delete-btn').prop('disabled', false);
}

function disableButtons(row) {
    row.find('.edit-btn').prop('disabled', true);
    row.find('.delete-btn').prop('disabled', true);
}


$(document).on('click', '.edit-btn', function (event) {
    editRow(this, event);
});

$(document).on('click', '.save-btn', function (event) {
    saveRow(this, event);
});

$(document).on('click', '.cancel-btn', function (event) {
    cancelEditRow(this, event);
});

// ----------------------------------------------------------------------------------------------------------------
//                                  Authentication Tab
// ----------------------------------------------------------------------------------------------------------------
// This function could be triggered when the Authentication Tab is opened
// $('#user').click(function () {
//     location.reload(true);
//     document.getElementById("user").value = ' active';
// });
// let originalValues = {}
// function loadUserData() {
//     $.ajax({
//         url: '/authentication',  // Adjust the URL as per your route
//         type: 'GET',
//         success: function (response) {
//             // console.log("Response:", response)
//             // Assuming 'response' contains the HTML with the user table
//             populateUserTable(response.users, response.roles);
//             $('#create_user').off('click').on('click', function (event) {
//                 addUser(response.users, response.roles);
//             });
//         },
//         error: function (error) {
//             console.log("Error fetching user data:", error);
//             // Handle errors appropriately
//         }
//     });
// }



// $('document').ready(function () {
//     loadUserData();
//     $("#loadUserData").hide();
// });

// function populateUserTable(users, roles) {
//     var tableBody = $('#utresultBody');
//     tableBody.empty(); // Clear existing table rows
//     var tableHTML = '';

//     // Loop through the users array
//     users.forEach(function (user) {
//         tableHTML += '<tr id="user-row-' + user.id + '">';
//         tableHTML += '<td>' + user.fullName + '</td>'; // Full Name
//         tableHTML += '<td>' + user.userName + '</td>'; // User Name
//         tableHTML += '<td>' + user.email + '</td>'; // Email
//         tableHTML += '<td class="non-editable-password">' + user.password + '</td>'; // Password
//         tableHTML += '<td>' + createRoleDropdown(user.role, roles) + '</td>'; // Role Dropdown
//         // tableHTML += '<td>' + createStatusCheckboxes(user.id, user.Active) + '</td>'; // Status Checkboxes
//         tableHTML += '<td class="actions">';
//         tableHTML += createActionButtons(user.id); // Action Buttons
//         tableHTML += '</td>';
//         tableHTML += '</tr>';
//     });
//     tableBody.html(tableHTML); // Inject the generated HTML into the table body
//     // tableBody.append(tableHTML);
//     $('#utresultTable').show(); // Show the table
//     $('#utresultTable thead').show(); // Show the table header
// }

// $('#utresultBody').on('click', 'td', function (event) {
//     event.stopPropagation();
// });


// function editUser(userId) {

//     var row = $('#user-row-' + userId);
//     // console.log("editUser called for user ID:", userId, "Caller:", editUser.caller);
//     // Avoid re-initializing edit mode if it's already active
//     if (row.find('.save1-btn').length > 0) {
//         return;
//     }

//     var originalValues = {
//         textValues: [],
//         roleValue: '',
//         statusValues: []
//     };

//     // console.log("Original Vlaues Initially: ",originalValues)

//     row.find('td:not(:last-child)').each(function (index, td) {
//         var cell = $(td);

//         // Store text values for text cells
//         if (index < 3) { // Assuming first 4 columns are text
//             originalValues.textValues.push(cell.text());
//             cell.html('<input type="text" class="form-control" value="' + cell.text() + '">');
//         }

//         var roleDropdown = row.find('select');

//         // console.log(roleDropdown); // Check what is being selected
//         if (roleDropdown.length > 0) {
//             originalValues.roleValue = roleDropdown.val();
//             roleDropdown.prop('disabled', false);
//         } else {
//             console.log('Role dropdown not found for user id:', userId);
//         }

//         // Store and enable status checkboxes
//         if (cell.find('input').length > 0) {
//             cell.find('input').each(function () {
//                 originalValues.statusValues.push($(this).is(':checked'));
//             });
//             cell.find('input').prop('disabled', false);
//         }
//     });
//     row.data('original-values', originalValues);
//     // console.log("Edit User Original Data:", originalValues);

//     // Replace Action Buttons
//     var actionCell = row.find('td:last');
//     actionCell.empty();
//     actionCell.append('<button class="save1-btn" id="save1-btn" onclick="saveEditedUser(' + userId + ')">Save</button>');
//     actionCell.append('<button class="cancel1-btn" id="cancel1-btn" onclick="cancelEditUser(' + userId + ')">Cancel</button>');
//     return;

// }
// var email = document.querySelectorAll('#utresultBody tr td')

// function cancelEditUser(userId) {
//     var row = $('#user-row-' + userId);
//     var originalValues = row.data('original-values');
//     // console.log("Cancel Edit User Original Data:", originalValues)

//     // Revert the text input fields to original values
//     row.find('td:not(:nth-child(5)):not(:nth-child(6))').each(function (index, td) {
//         $(td).text(originalValues.textValues[index]);
//     });

//     // Revert the role dropdown
//     var roleSelect = row.find('select');
//     // console.log("Role Value:", originalValues.roleValue)
//     roleSelect.val(originalValues.roleValue);

//     // Revert the status checkboxes
//     var statusCheckbox = row.find('input[type="checkbox"]');
//     // Assuming the last element in statusValues refers to the checkbox status
//     var checkboxIndex = originalValues.statusValues.length - 1;
//     statusCheckbox.prop('checked', originalValues.statusValues[checkboxIndex]);


//     // Re-disable inputs, select, and checkboxes
//     row.find('input, select').prop('disabled', true);

//     // Restore original action buttons
//     var actionCell = row.find('td:last');
//     actionCell.empty();
//     actionCell.append(createActionButtons(userId));

//     // Optionally, clear the stored original values
//     row.removeData('original-values');
// }



// function saveEditedUser(userId) {
//     var row = $('#user-row-' + userId);
//     // var row = $(button).closest("tr");
//     // var userId = row.data('user-id');
//     // var userId = row.attr('data-user-id');

//     // console.log("Row element:", row);
//     // console.log("User ID found:", userId);
//     if (!userId) {
//         console.error('User ID not found.', row.data());
//         return;
//     }

//     var SendData = {
//         "FullName": "",
//         "UserName": "",
//         "Email": "",
//         "Password": "",
//         "Role_id": "",
//         "Status": ""
//     };

//     // Iterate through each cell to collect data and update cell text
//     row.find('td:not(:last-child)').each(function (index) {
//         var cell = $(this);
//         var input = cell.find('input, select');
//         var key = Object.keys(SendData)[index];

//         if (input.length > 0) {
//             if (input.is('input[type="checkbox"]')) {
//                 // Check the checkbox status and set "Active" or "Inactive"
//                 SendData["Status"] = input.is(':checked') ? "Active" : "Inactive";
//             } else {
//                 // For other inputs and selects, just save the value
//                 var value = input.val();
//                 SendData[key] = value;
//                 // Update the cell text if it's an input field
//                 if (!input.is('select')) {
//                     cell.text(value);
//                 }
//             }
//         }
//     });
//     row.find('input, select').prop('disabled', true);

//     // console.log("Edited Row Data:", SendData);

//     // console.log("Sending Data to Server:", SendData);
//     // console.log("URL: '/save_User/' + userId", '/save_User/' + userId);

//     // AJAX call to save data
//     $.ajax({
//         // console.log("URL Ajax", url),
//         url: '/save_User/' + userId,
//         type: 'POST',
//         contentType: 'application/json',
//         data: JSON.stringify(SendData),
//         success: function (response) {
//             // console.log("User saved:", response);
//             loadUserData();
//             location.reload();
//         },
//         error: function (error) {
//             console.error("Error saving user data:", error);
//             alert("Please provide unique value for Username and Email!")
//             location.reload()
//         }
//     });
//     // Reset the action buttons to the default state
//     var actionCell = row.find('td:last');
//     actionCell.empty();
//     actionCell.append(createActionButtons(userId));
// }

// function addUser(users, roles) {
//     var roleDropdownHtml = '<select>';
//     // console.log("Roles:",roles)
//     roles.forEach(function (role) {
//         roleDropdownHtml += '<option value="' + role + '">' + role + '</option>';
//     });
//     roleDropdownHtml += '</select>';

//     // Create a new row for adding a component
//     var newRow = '<tr class="newUser-row">' +
//         '<td><input type="text" class="editable" placeholder="Enter Full Name" maxlength="20"></td>' +
//         '<td><input type="text" class="editable" placeholder="Enter Username" maxlength="20"></td>' +
//         '<td><input type="text" class="editable" placeholder="Enter Email" maxlength="20"></td>' +
//         '<td><input type="text" class="editable" placeholder="Enter Password" maxlength="20"></td>' +
//         '<td>' + roleDropdownHtml + '</td>' +
//         // '<td><input type="checkbox" id="new-user-status"></td>'+
//         '<td class="actions">' +
//         '<button class="save1-btn" id="save1-btn" onclick="saveNewUser(this,event)">Save</button>' +
//         '<button class="cancel1-btn" id="cancel1-btn" onclick="cancelNewUser(this,event)">Cancel</button>' +
//         '</td>' +
//         '</tr>';
//     // Append the new row at the end of the table
//     $('#utresultBody').append(newRow);

// }

// function saveNewUser(button) {
//     var newRow = $(button).closest("tr.newUser-row");
//     var fullName = newRow.find('td input').eq(0).val(); // Full Name
//     var userName = newRow.find('td input').eq(1).val(); // User Name
//     var email = newRow.find('td input').eq(2).val(); // Email
//     var password = newRow.find('td input').eq(3).val();

//     if (!fullName || fullName.length < 3 || fullName.length > 20) {
//         alert("Full name must be between 3 and 20 characters.");
//         return;
//     }
//     if (!userName || userName.length < 3 || userName.length > 20) {
//         alert("Username must be between 3 and 20 characters.");
//         return;
//     }
//     if (!email || email.length < 3 || email.length > 30) {
//         alert("Email must be between 3 and 30 characters.");
//         return;
//     }
//     if (!password || password.length < 3 || password.length > 20) {
//         alert("Password must be between 3 and 20 characters.");
//         return;
//     }
//     var SendData = {
//         "FullName": newRow.find('td input').eq(0).val(), // Full Name
//         "UserName": newRow.find('td input').eq(1).val(), // User Name
//         "Email": newRow.find('td input').eq(2).val(), // Email
//         "Password": newRow.find('td input').eq(3).val(), // Password
//         "Role_id": newRow.find('td select').val(), // Role Dropdown Value
//         // "Status": newRow.find('td input[type="checkbox"]').is(':checked') ? "Active" : "Inactive" // Status Checkbox
//     };

//     // console.log("Sending Data to Server:", SendData);

//     // AJAX call to send data to the server
//     $.ajax({
//         url: '/create_user',  // Adjust the URL as per your server endpoint
//         type: 'POST',
//         contentType: 'application/json',
//         data: JSON.stringify(SendData),
//         success: function (response) {
//             // console.log("User saved:", response);
//             // Add any success handling logic here
//             newRow.remove(); // Remove the new user row after saving
//             loadUserData();
//             location.reload();
//         },
//         error: function (error) {
//             console.error("Error saving new user data:", error);
//             newRow.remove();
//             alert("Please provide the Unique values in Username and Email!")
//             // Handle the error scenario
//         }
//     });
// }


// function cancelNewUser(button) {
//     // Remove the new row upon cancellation
//     var newRow = $(button).closest('tr.newUser-row');
//     newRow.remove();
// }

// function deleteUser(userId) {
//     // var row = $(button).closest("tr");
//     var row = $('#user-row-' + userId);
//     // var userId = row.find('td:first').text();
//     // console.log("User ID found:", userId);
//     if (!userId) {
//         console.error('User ID not found.', row.data());
//         return;
//     }
//     // console.log("URL: '/delete_User/' + userId", '/delete_User/' + userId);
//     var confirmationMessage = "Are you sure you want to delete this user?";
//     var confirmation = window.confirm(confirmationMessage);
//     if (confirmation) {
//         // AJAX call to save data
//         $.ajax({
//             // console.log("URL Ajax", url),
//             url: '/delete_User/' + userId,
//             type: 'POST',
//             contentType: 'application/json',
//             data: { userId: userId },
//             success: function (response) {
//                 // console.log('Record deleted:', response);
//                 row.remove();
//                 loadUserData();
//                 location.reload();
//             },
//             error: function (error) {
//                 console.error("Unexpected error occred while deleting the User:", error);
//                 alert("Unexpected error occred while deleting the User!");
//                 location.reload();
//             }
//         });
//     }
// }




// // You might also want to adjust your createActionButtons function to add classes for easier selection
// function createActionButtons(userId) {
//     return '<button class="edit1-btn" onclick="editUser(' + userId + ')">Edit</button>' +
//         '<button class="delete1-btn" onclick="deleteUser(' + userId + ')">Delete</button>';
// }

// function createRoleDropdown(currentRole, roles) {
//     var dropdownHtml = '<select disabled>';
//     roles.forEach(function (role) {
//         dropdownHtml += '<option value="' + role + '"' +
//             (role === currentRole ? ' selected' : '') + '>' +
//             role + '</option>';
//     });
//     dropdownHtml += '</select>';
//     return dropdownHtml;
// }
// function createStatusCheckboxes(userId, status) {
//     console.log("Status Received: ",status)
//     var isActive = status === 'Active'; // Uncomment and use your logic to set isActive

//     console.log("Active?", isActive)
//     // Adjust the HTML to include onclick event handlers for the checkboxes
//     var checkboxesHtml = '<input type="checkbox" id="statusActive-' + userId + 
//         '" name="statusActive" ' + (isActive ? 'checked' : '') + 
//         ' onclick="toggleCheckbox(' + userId + ', this.checked)" disabled>';

//     return checkboxesHtml;
// }

// // function to toggle the state of the checkboxes
// function toggleCheckbox(userId, isChecked) {
//     $('#statusActive-' + userId).prop('checked', isChecked);
// }

//------------------------------------------------------------------------------------------------------------
//                                 Clue Table Tab Functionality
//------------------------------------------------------------------------------------------------------------

$('#clue').click(function () {
    location.reload();
    document.getElementById("clue").value = ' active';
});
if (localStorage.getItem('lastOpenedTab') == 'Clue') {
    $('#fetchclue').click();
}
if (localStorage.getItem('lastOpenedTab') == "ClueTab") {
    $('#fetchclue').click();
    $('#Clue').addClass("addedclass");
}

async function fetchClueData() {
    try {
        const maskToken = $('#filter-mask-token').val();
        const query = $('#search-input').val().toLowerCase();

        const response = await $.ajax({
            type: "POST",
            url: "/ClueComponents",
            data: JSON.stringify({ maskToken, query }),  // Send both mask token and search query
            contentType: 'application/json',
        });

        var filteredComponents = response.result;

        if (query) {
            // Filter components based on the search query
            var filteredResults = {};
            for (var key in filteredComponents) {
                if (filteredComponents.hasOwnProperty(key) && key.toLowerCase().includes(query)) {
                    filteredResults[key] = filteredComponents[key];
                }
            }
            filteredComponents = filteredResults;
        }

        populateClueTable(filteredComponents);
        $('#fetchclue').hide();
    } catch (error) {
        console.error('Error:', error);
    }
}



// Variables for pagination
var currentPage = 1;
var rowsPerPage = 10;

// Function to populate the clue table
function populateClueTable(components) {
    var tableBody = $('#clueresultBody');
    var tableHTML = '';

    var totalRows = Object.keys(components).length;
    var totalPages = Math.ceil(totalRows / rowsPerPage);

    var start = (currentPage - 1) * rowsPerPage;
    var end = start + rowsPerPage;
    var pageComponents = Object.keys(components).slice(start, end);

    pageComponents.forEach(function (key) {
        tableHTML += `<tr>
                        <td>${key}</td>
                        <td>${components[key]}</td>
                        <td class="actions">
                            <button class="edit-btn" onclick="editClueRow(this, event)">Edit</button>
                            <button class="delete-btn" onclick="deleteClueRow(this,event)">Delete</button>
                        </td>
                    `;
    });

    tableBody.html(tableHTML);
    $('#clueresultTable').show();
    $('#clueresultTable thead').show();

    renderPaginationControls(totalPages);
}


function deleteClueRow(button, event) {
    event.preventDefault();

    // Find the row to be deleted
    const row = $(button).closest('tr');
    const componentDesc = row.find('td:first').text();  // Assuming the first column contains component_desc

    // Ask for confirmation before proceeding
    const confirmation = confirm(`Are you sure you want to delete the Token : "${componentDesc}"?`);

    if (!confirmation) {
        
        return;  // Exit the function if the user cancels the deletion
    }

    // Send an AJAX request to delete the row from the database
    $.ajax({
        type: "POST",
        url: "/deleteClue",  // Server endpoint to handle deletion
        data: JSON.stringify({ component_desc: componentDesc }),  // Send the component_desc to identify the row
        contentType: 'application/json',
        success: function (response) {
            if (response.success) {
                // Remove the row from the table
                row.remove();
                console.log('Row successfully deleted');
            } else {
                console.log('Failed to delete row');
            }
        },
        error: function (error) {
            console.error('Error deleting row:', error);
        }
    });
}




// Function to render pagination controls
function renderPaginationControls(totalPages) {
    var pagination = $('#pagination');
    var paginationHTML = '';

    if (totalPages > 1) {
        if (currentPage > 1) {
            paginationHTML += `<button onclick="changePage(${currentPage - 1})" name="previous"><i class="bx bxs-left-arrow"></i> Previous</button>`;
        }

        for (var i = 1; i <= totalPages; i++) {
            if (i === 1 || i === totalPages || (i >= currentPage - 2 && i <= currentPage + 2)) {
                paginationHTML += `<button class="${i === currentPage ? 'active' : ''} pagebtn" onclick="changePage(${i})">${i}</button>`;
            } else if (i === 2 && currentPage > 3) {
                paginationHTML += '<span>...</span>';
            } else if (i === totalPages - 1 && currentPage < totalPages - 2) {
                paginationHTML += '<span>...</span>';
            }
        }

        if (currentPage < totalPages) {
            paginationHTML += `<button onclick="changePage(${currentPage + 1})" name="next">Next <i class="bx bxs-right-arrow"></i></button>`;
        }
    }

    pagination.html(paginationHTML);
}

// Function to change the current page
function changePage(page) {
    currentPage = page;
    fetchClueData();
}
function generateDropdownHTML(selectedOption, options) {
    return `<select class="editable">
                ${options.map(option => `<option value="${option}" ${option === selectedOption ? 'selected' : ''}>${option}</option>`).join('')}
            </select>`;
}
// Function to fetch dropdown options
let cachedDropdownOptions = null;

async function fetchDropdownOptions() {
    if (cachedDropdownOptions) {
        return cachedDropdownOptions;
    }

    try {
        const data = await $.ajax({
            url: '/get_token',
            method: 'GET',
        });
        cachedDropdownOptions = data;
        return data;
    } catch (error) {
        console.error('Failed to fetch dropdown options:', error);
        throw error;
    }
}


// Function to edit a clue row         (code for only Dropdown)
// async function editClueRow(button, event) {
//     event.stopPropagation();
//     event.preventDefault();

//     var row = $(button).closest("tr");
//     var cells = row.find("td:not(.actions)");

//     var dropdownOptions = [];
//     try {
//         dropdownOptions = await fetchDropdownOptions();
//     } catch (error) {
//         return;
//     }

//     var CluecomponentCell = cells.eq(0);
//     CluecomponentCell.attr('data-old-value', CluecomponentCell.text());

//     var descriptionClueCell = cells.eq(1);
//     descriptionClueCell.attr('data-old-value', descriptionClueCell.text());

//     var dropdownHTML = `<select class="editable">
//                         ${dropdownOptions.map(option => `<option value="${option}" ${option === descriptionClueCell.text() ? 'selected' : ''}>${option}</option>`).join('')}
//                        </select>`;
//     descriptionClueCell.html(dropdownHTML);

//     var actionsCell = row.find('.actions');
//     actionsCell.html('<button class="save-btn" onclick="saveClueRow(this, event)">Save</button>' +
//         '<button class="cancel-btn" onclick="cancelClueEditRow(this, event)">Cancel</button>');
//     enableClueButtons(row);
// }

async function editClueRow(button, event) {
    event.stopPropagation();
    event.preventDefault();

    var row = $(button).closest("tr");
    var cells = row.find("td:not(.actions)");

    var dropdownOptions = [];
    try {
        dropdownOptions = await fetchDropdownOptions();
    } catch (error) {
        return;
    }

    var CluecomponentCell = cells.eq(0);
    CluecomponentCell.attr('data-old-value', CluecomponentCell.text());
    var descriptionClueCell = cells.eq(1);
    descriptionClueCell.attr('data-old-value', descriptionClueCell.text());

    // Convert first cell to an editable text field
    CluecomponentCell.html(`<input type="text" class="editable" value="${CluecomponentCell.text()}" required maxlength="10">`);

    // Convert second cell to a dropdown
    descriptionClueCell.html(generateDropdownHTML(descriptionClueCell.text(), dropdownOptions));

    var actionsCell = row.find('.actions');
    actionsCell.html('<button class="save-btn" onclick="saveClueRow(this, event)">Save</button>' +
        '<button class="cancel-btn" onclick="cancelClueEditRow(this, event)">Cancel</button>');
    enableButtons(row);
    const editButtons = document.querySelectorAll('.edit-btn');
    const delButtons = document.querySelectorAll('.delete-btn');
    // Loop through the NodeList and disable each button
    editButtons.forEach(function (button) {
        button.disabled = true;
    });
    delButtons.forEach(function (button) {
        button.disabled = true;
    });
    document.getElementById("addClueComponent").disabled = true;
}


// Function to cancel the edit of a clue row
function cancelClueEditRow(button, event) {
    var row = $(button).closest('tr');
    var cells = row.find('td:not(.actions)');

    cells.each(function () {
        var cell = $(this);
        var oldValue = cell.attr('data-old-value');
        cell.text(oldValue);
    });

    var actionsCell = row.find('.actions');
    actionsCell.html('<button class="edit-btn" onclick="editClueRow(this, event)">Edit</button><button class="delete-btn" onclick="deleteClueRow(this,event)">Delete</button>');
    enableButtons(row);
    const editButtons = document.querySelectorAll('.edit-btn');
    const delButtons = document.querySelectorAll('.delete-btn');
    // Loop through the NodeList and disable each button
    editButtons.forEach(function (button) {
        button.removeAttribute('disabled');
    });
    delButtons.forEach(function (button) {
        button.removeAttribute('disabled');
    });
    document.getElementById("addClueComponent").disabled = false;
}

// Function to save the edited clue row  (code for only Dropdown)
// function saveClueRow(button, event) {
//     var row = $(button).closest('tr');
//     var cells = row.find('td:not(.actions)');

//     var componentCell = cells.eq(0);
//     var newComponent = componentCell.text();

//     var descriptionCell = cells.eq(1);
//     var dropdown = descriptionCell.find('select');
//     var newDescription = dropdown.val();
//     descriptionCell.text(newDescription);

//     var payload = {
//         oldComponent: componentCell.attr('data-old-value'),
//         oldDescription: descriptionCell.attr('data-old-value'),
//         newComponent: newComponent,
//         newDescription: newDescription
//     };

//     $.ajax({
//         url: '/update_clue',
//         method: 'POST',
//         contentType: 'application/json',
//         data: JSON.stringify(payload),
//         success: function (response) {
//             console.log(response);
//         },
//         error: function (error) {
//             console.error('Error updating clue:', error);
//         }
//     });

//     var actionsCell = row.find('.actions');
//     actionsCell.html('<button class="edit-btn" onclick="editClueRow(this, event)">Edit</button>' +
//         '<button class="delete-btn" onclick="deleteClueRow(this, event)">Delete</button>');
//     enableClueButtons(row);
// }

function saveClueRow(button, event) {
    event.stopPropagation();
    var row = $(button).closest('tr');
    var cells = row.find('td:not(.actions)');

    var componentCell = cells.eq(0);
    var componentInput = componentCell.find('input');
    var newComponent = componentInput.val();  // Get the value from the input field

    if (newComponent.trim() === "") {
        alert('Token name cannot be empty.');
        return;
    }

    var descriptionCell = cells.eq(1);
    var dropdown = descriptionCell.find('select');
    var newDescription = dropdown.val();  // Get the selected value from the dropdown

    var payload = {
        oldComponent: componentCell.attr('data-old-value'),
        oldDescription: descriptionCell.attr('data-old-value'),
        newComponent: newComponent,
        newDescription: newDescription
    };

    $.ajax({
        url: '/update_clue',
        method: 'POST',
        contentType: 'application/json',
        data: JSON.stringify(payload),
        success: function (response) {
            if (response.status === 'duplicate') {
                alert('Token already exists in the database.');
            } else if (response.status === 'success') {
                // Update the table cells only if the update is successful
                componentCell.text(newComponent);  // Set the cell text to the new component value
                descriptionCell.text(newDescription);  // Set the cell text to the new description value

                var actionsCell = row.find('.actions');
                actionsCell.html('<button class="edit-btn" onclick="editClueRow(this, event)">Edit</button>' +
                    '<button class="delete-btn" onclick="deleteClueRow(this, event)">Delete</button>');
                enableButtons(row);
            } else {
                alert('Error updating clue: ' + response.message);
            }
        },
        error: function (error) {
            // console.error('Error updating clue:', error);
            alert('Token Alreadyt Exist.');
        }
    });
}




// --------------------Adding New Mask Token------------------------------------

async function addClueComponent() {
    var dropdownOptions = [];
    try {
        dropdownOptions = await fetchDropdownOptions();
    } catch (error) {
        console.error('Error fetching dropdown options:', error);
        return;
    }

    var newRow = '<tr class="new-row">' +
        '<td><input type="text" class="editable" placeholder="New Component" maxlength="20" required></td>' +
        '<td>' + generateDropdownHTML("", dropdownOptions) + '</td>' +
        '<td class="actions">' +
        '<button class="save-btn" id="clueSave-btn" onclick="saveNewClueRow(this,event)">Save</button>' +
        '<button class="cancel-btn" id="cancel-btn" onclick="cancelAddRow(this,event)">Cancel</button>' +
        '</td>' +
        '</tr>';

    $('#clueresultBody').append(newRow);
    const editButtons = document.querySelectorAll('.edit-btn');
    const delButtons = document.querySelectorAll('.delete-btn');
    document.getElementById('addClueComponent').disabled = true;
    // Loop through the NodeList and disable each button
    editButtons.forEach(function (button) {
        button.disabled = true;
    });
    delButtons.forEach(function (button) {
        button.disabled = true;
    });
}


function saveNewClueRow(button,event) {
    event.stopPropagation();
    var newRow = $(button).closest('tr.new-row');
    var cells = newRow.find('td:not(.actions)');
    var newMask = cells.eq(0).find('input').val().toUpperCase();
    var newToken = cells.eq(1).find('select').val();

    if (newMask.trim() === "") {
        alert('Component description cannot be empty.');
        return;
    }

    if (newMask) {
        $.ajax({
            type: "POST",
            url: "/add_new_ClueComponent",
            contentType: "application/json",
            data: JSON.stringify({ newMask: newMask, newToken: newToken }),
            success: function (response) {
                if (response.error) {
                    alert(response.error);
                    location.reload();
                } else {
                    alert(response.message);
                    newRow.remove();
                    fetchClueData();
                    // location.reload();
                }
            },
            error: function (error) {
                console.error('Error adding new record:', error);
            }
        });
    } else {
        alert('Please provide both Token Name and Mask Token values.');
    }
    document.getElementById("addClueComponent").disabled = false;
}




async function searchClue() {
    var query = $('#search-input').val().toLowerCase();

    try {
        const maskToken = $('#filter-mask-token').val();  // Get the selected mask token

        const response = await $.ajax({
            type: "POST",
            url: "/ClueComponents",
            data: JSON.stringify({ maskToken, query }),  // Send both mask token and search query
            contentType: 'application/json',
        });

        var filteredComponents = response.result;

        if (query) {
            // Filter components based on the search query
            var filteredResults = {};
            for (var key in filteredComponents) {
                if (filteredComponents.hasOwnProperty(key) && key.toLowerCase().includes(query)) {
                    filteredResults[key] = filteredComponents[key];
                }
            }
            filteredComponents = filteredResults;
        }

        currentPage = 1; // Reset to the first page
        populateClueTable(filteredComponents);
        $('#fetchclue').hide();
    } catch (error) {
        console.error('Error:', error);
    }
}

document.getElementById('search-input').addEventListener('input', function () {
    $('#search-btn').click();
});

// ------------------------------Filter------------------------------------------------/
$(document).ready(async function () {
    await populateMaskTokenDropdown();
    fetchClueData();  // Initial fetch of the clue data
});

async function populateMaskTokenDropdown() {
    try {
        const dropdownOptions = await fetchDropdownOptions();
        const filterDropdown = $('#filter-mask-token');
        dropdownOptions.forEach(option => {
            filterDropdown.append(`<option value="${option}">${option}</option>`);
        });
    } catch (error) {
        console.error('Error fetching dropdown options:', error);
    }
}

function applyFilters() {
    currentPage = 1;  // Reset to the first page when applying filters
    fetchClueData();
}