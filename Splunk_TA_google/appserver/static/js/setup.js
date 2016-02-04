$(document).ready(function() {
    $(".ManagerPageTitle").text("Splunk Add-on for Google");
    // 0) global vars
    // FIXME for the ebox_id, it will be different from TA to TA.
    var allSettingsEboxId = "#\\/google_input_setup\\/google_settings\\/google_settings\\/all_settings_id";

    var appname = Splunk.util.getCurrentApp();
    // 1) Load dependent css and javascript
    $("<link>").attr({
        rel: "stylesheet",
        type: "text/css",
        href: "/en-US/static/app/" + appname + "/css/setup.css",
    }).appendTo("head");

    // 2) Append new html
    var originFormWrapper = $(".formWrapper");
    originFormWrapper.css("display", "none");
    originFormWrapper.before(return_page());
    // $("#proxySettingId").css("display", "none");
    // originFormWrapper.after(return_page());
    // originFormWrapper.remove();

    function htmlEscape(str) {
        return String(str)
                   .replace(/&/g, '&amp;')
                   .replace(/"/g, '&quot;')
                   .replace(/'/g, '&#39;')
                   .replace(/</g, '&lt;')
                   .replace(/>/g, '&gt;');
    }

    function htmlUnescape(value){
        return String(value)
                   .replace(/&quot;/g, '"')
                   .replace(/&#39;/g, "'")
                   .replace(/&lt;/g, '<')
                   .replace(/&gt;/g, '>')
                   .replace(/&amp;/g, '&');
    }

    function isTrue(value) {
        if (value === undefined) {
            return 0;
        }

        value = value.toUpperCase();
        var trues = ["1", "TRUE", "T", "Y", "YES"];
        return trues.indexOf(value) >= 0;
    }

    function updateHeaders(tableId, cols){
        var theadTr = $("#" + tableId + " .tableHead>tr");
        cols.forEach(function(col, i){
            var td = $("<td><span data-idx='" + i + "'>" + col.name+"</span></td>");
            theadTr.append(td);
        });

        var td = $("<td><span data-idx='" + cols.length + "'>Action</span></td>");
        theadTr.append(td);
        hideColumns(tableId, cols);
    };

    function setCheckBox(boxId, value) {
        if (value === undefined) {
            value = "0";
        }
        value = value.toLowerCase();
        if (value == "1" || value == "true" || value == "yes") {
            $("#" + boxId).prop("checked", true);
        } else {
            $("#" + boxId).prop("checked", false);
        }
    };

    function updateGlobalSettings(settings) {
        // Global settings
        if (settings.global_settings === undefined) {
            return;
        }

        // $("#index_id").val(settings["global_settings"]["index"]);
        $("#log_level_id").val(settings["global_settings"]["log_level"]);

        // Proxy settings
        if (settings.proxy_settings === undefined) {
            return;
        }

        setCheckBox("enable_proxy_id", settings["proxy_settings"]["proxy_enabled"]);
        $("#proxy_url_id").val(settings["proxy_settings"]["proxy_url"]);
        $("#proxy_port").val(settings["proxy_settings"]["proxy_port"]);
        $("#proxy_username").val(settings["proxy_settings"]["proxy_username"]);
        $("#proxy_port").val(settings["proxy_settings"]["proxy_port"]);
        $("#proxy_type").val(settings["proxy_settings"]["proxy_type"]);

        setCheckBox("proxy_rdns_id", settings["proxy_settings"]["proxy_rdns"]);
    };

    function updateSettings(cols, credentialSettings) {
        var creds = [];
        var credsMap = {};

        if (credentialSettings) {
            for (var k in credentialSettings) {
                if (isTrue(credentialSettings[k].removed)) {
                    continue;
                }
                var rec = [k];
                for (var i = 1; i < cols.length; i++) {
                    var val = credentialSettings[k][cols[i].id]
                    if (val === undefined || val == null) {
                        val = "";
                    }
                    rec.push(val);
                }
                creds.push(rec);
                credsMap[k] = rec;
            }
        }

        return {
            "data": creds,
            "dataMap": credsMap,
        };
    };

    function hideColumns(tableId, cols) {
        for (var i = 0; i < cols.length; i++) {
            if (cols[i].hide) {
                $("#" + tableId + " td:nth-child(" + (i + 1) + "),th:nth-child(" + i + ")").hide();
            }
        }
    };

    function editRow(e) {
        currentAction = "Edit";
        var rowIdAndTableId = e.target.id.split("``");
        var table = tables[rowIdAndTableId[1]];
        var credName = $("input#" + table.columns[0].id);
        credName.prop("readonly", true);
        credName.css("background-color", "#D3D3D3");

        var did = undefined;
        for (var dialogId in dialogs) {
            if (dialogs[dialogId].table.id == table.id) {
                did = dialogId;
                break;
            }
        }

        var dialog = $("#" + did);
        dialog.text(dialog.text().replace("Add", "Edit"));
        showDialog(did);
        table.columns.forEach(function(c, i){
            $("input#" + c.id).val(table.dataMap[rowIdAndTableId[0]][i]);
        });
        return false;
    };

    function deleteRow(e) {
        var rowIdAndTableId = e.target.id.split("``");
        var table = tables[rowIdAndTableId[1]];
        for (var i = 0; i < table.data.length; i++) {
            if (table.data[i][0] == rowIdAndTableId[0]) {
                table.data.splice(i, 1);
                delete table.dataMap[rowIdAndTableId[0]];
                break;
            }
        }
        updateTable(table.id, table.data, table.columns);
        return false;
    };

    function updateTable(tableId, tableData, cols){
        if (!tableData) {
            return
        }

        var tbody = $("#" + tableId + " .tableBody");
        tbody.empty();
        tableData.forEach(function(row){
            var tr = $("<tr></tr>");
            row.forEach(function(cell){
                var td = $("<td>" + cell + "</td>");
                tr.append(td);
            });

            var id = row[0] + "``" + tableId;

            var remove_hyperlink_cell= $("<a>", {
                "href": "#",
                "id": id,
                click: deleteRow,
            }).append("Delete");

            var edit_hyperlink_cell= $("<a>", {
                "href": "#",
                "id": id,
                click: editRow,
            }).append("Edit");

            var td = $("<td>").append(remove_hyperlink_cell).append(" | ").append(edit_hyperlink_cell);
            tr.append(td);
            tbody.append(tr);
        });
        hideColumns(tableId, cols);
    };

    function showDialog(dialogId) {
        $("." + dialogId).css("display", "block");
        $(".shadow").css("display", "block");
    };

    function hideDialog(dialogId) {
        $("." + dialogId).css("display", "none");
        $(".shadow").css("display", "none");
    };

    function hideDialogHandler(e) {
        var btnIdToDialogId = {
            "credDialogBtnCancel": "credDialog",
            "dataCollectionDialogBtnCancel": "dataCollectionDialog",
        };
        hideDialog(btnIdToDialogId[e.target.id]);
        return false;
    };

    function verifyDialogHandler(e) {
        if (e.target.id == "dataCollectionDialogBtnSave") {
            var ids = ["#google_project", "#google_subscription"];
            for (var i = 0; i < ids.length; i++) {
                if ($(ids[i]).val() == "placeholder") {
                    // FIXME more GUI hint
                    return false;
                }
            }
        }
        return true;
    };

    function clearFlag(){
        $("table thead td span").each(function() {
            $(this).removeClass("asque");
            $(this).removeClass("desque");
        });
    };

    function getJSONResult() {
        var result = {};
        // 1. Global Settings
        var index = $("#index_id").val();
        var log_level = $("#log_level_id").val();
        result["global_settings"] = {
            "index": index,
            "log_level": log_level,
        }

        // 2. Proxy Settings
        var proxy_settings = {
            "proxy_enabled": "0",
            "proxy_url": "",
            "proxy_port": "",
            "proxy_username": "",
            "proxy_password": "",
            "proxy_type": "",
            "proxy_rdns": "0",
        }

        var proxy_enabled = $("#enable_proxy_id")[0].checked;
        if (proxy_enabled) {
            proxy_settings["proxy_enabled"] = "1";
            proxy_settings["proxy_host"] = $("#proxy_url_id").val();
            proxy_settings["proxy_port"] = $("#proxy_port_id").val();
            proxy_settings["proxy_username"] = $("#proxy_username_id").val();
            proxy_settings["proxy_password"] = $("#proxy_password_id").val();
            proxy_settings["proxy_type"] = $("#proxy_type_id").val();
            if ($("#proxy_rdns_id")[0].checked) {
                proxy_settings["proxy_rdns"] = "1";
            }
        } else {
            proxy_settings["proxy_enabled"] = "0";
        }
        result["proxy_settings"] = proxy_settings;

        // 3. Credential Settings and Data Collection Settings
        var settings = {
            "credential_settings": tables.credTable,
            "data_collection_settings": tables.dataCollectionTable,
        };

        for (var k in settings) {
            result[k] = {};
            var table = settings[k];
            for (var i = 0; i < table.data.length; i++){
                var temp = {};
                table.columns.forEach(function(c, j){
                    temp[c.id] = table.data[i][j];
                });
                result[k][temp[table.columns[0].id]] = temp;
                delete temp[table.columns[0].id];
            }
        }

        // return JSON.stringify(result);
        return result;
    }

    function showHideProxy() {
        if ($("#enable_proxy_id")[0].checked) {
            $(".proxy").css("display","block");
        } else {
            $(".proxy").css("display","none");
        }
    };

    function registerBtnClickHandler(did) {
        $("#" + dialogs[did].btnId).click(function(){
            currentAction = "New";
            var table = dialogs[did]["table"];
            $("input#" + table.columns[0].id).prop("readonly", false);
            table.columns.forEach(function(c, j){
                if (c.id == "index") {
                    $("input#" + c.id).val("main");
                } else {
                    $("input#" + c.id).val("");
                }
            });
            $("input#" + table.columns[0].id).css("background-color", "rgb(255, 255, 255)");
            var dialog = $("#" + did);
            var saveBtnId = did + "BtnSave";
            dialog.text(dialog.text().replace("Edit", "Add"));
            showDialog(did);
            return false;
        });
    };

    // DATA...
    // Table header
    var columns = [{
        id: "credName",
        name: "Name",
        name_with_desc: "Google Credential Name",
        required: "required",
        hide: false,
        dialogHide: false,
    }, {
        id: "google_credentials",
        name: "Google Credentials",
        name_with_desc: "Google Service Credentials (json format)",
        required: "required",
        hide: false,
        dialogHide: false,
    }];

    var dataCollectionColumns = [{
        id: "dataCollectionName",
        name: "Name",
        name_with_desc: "Data Collection Name",
        required: "required",
        hide: false,
        dialogHide: false,
    }, {
        id: "google_credentials_name",
        name: "Google Credentials",
        name_with_desc: "Google Credentials",
        required: "required",
        hide: false,
        dialogHide: false,
    }, {
        id: "google_project",
        name: "Google Project",
        name_with_desc: "Project Name",
        required: "required",
        hide: false,
        dialogHide: false,
    }, {
        id: "google_subscription",
        name: "Google Topic Subscription",
        name_with_desc: "Topic Subscription",
        required: "required",
        hide: false,
        dialogHide: false,
    }, {
        id: "index",
        name: "Index",
        name_with_desc: "Index",
        required: "required",
        hide: false,
        dialogHide: false,
    }];

    var allSettings = $(allSettingsEboxId).val();
    allSettings = $.parseJSON(allSettings);
    updateGlobalSettings(allSettings);
    var creds = updateSettings(columns, allSettings.credential_settings);
    var dataCollections = updateSettings(dataCollectionColumns, allSettings.data_collection_settings);

    var tables = {
        "credTable": {
            "id": "credTable",
            "columns": columns,
            "data": creds.data,
            "dataMap": creds.dataMap,
        },
        "dataCollectionTable": {
            "id": "dataCollectionTable",
            "columns": dataCollectionColumns,
            "data": dataCollections.data,
            "dataMap": dataCollections.dataMap,
        },
    };

    for (var tableId in tables) {
        updateHeaders(tableId, tables[tableId].columns);
        hideColumns(tableId, tables[tableId].columns);
        updateTable(tableId, tables[tableId].data, tables[tableId].columns);
    }

    var currentAction = "New";
    var dialogs = {
        "credDialog": {
            "id": "credDialog",
            "btnId": "btnAdd",
            "formId": "credForm",
            "table": tables.credTable,
        },
        "dataCollectionDialog": {
            "id": "dataCollectionDialog",
            "btnId": "dataCollectionBtnAdd",
            "formId": "dataCollectionForm",
            "table": tables.dataCollectionTable,
        },
    };

    for (var dialogId in dialogs) {
        enjectDialogForm(dialogId, dialogs[dialogId].formId, dialogs[dialogId].table.columns);
        registerBtnClickHandler(dialogId);
    }

    function getCredentials(credName) {
        var data = tables.credTable.data;
        for (var i = 0; i < data.length; i++) {
            if (data[i][0] == credName) {
                return btoa(data[i][1]);
            }
        }
        return "";
    };

    function reinitDropdowns() {
        var ids = [
            ["#google_project", "Select Google PubSub project"],
            ["#google_subscription", "Select Google PubSub subscription"]
        ];
        for (var i = 0; i < ids.length; i++) {
            $(ids[i][0])
                .find("option")
                .remove()
                .end()
                .append($("<option></option>")
                        .attr("value", "placeholder")
                        .text(ids[i][1]));
            disable(ids[i][0]);
        }
    };

    function handleCredSelection(e) {
        var cred = getCredentials($(this).val());
        if (!cred) {
            reinitDropdowns();
            return;
        }

        var payload = {
            type: "GET",
            async: true,
            url: "/custom/Splunk_TA_google/proxy/servicesNS/admin/Splunk_TA_google/splunk_ta_google/google_projects?output_mode=json&count=-1&google_credentials=" + cred,
            success: function(data) {
                if (data.entry === undefined || data.entry.length == 0) {
                    // FIXME
                    return false;
                }
                $.each(data.entry[0].content.projects, function(key, value) {
                     $("#google_project").append(
                         $("<option></option>")
                         .attr("value", value)
                         .text(value));
                });
                enable("#google_project");
            },
            error: function(data) {
                // FIXME
                console.log(data);
            }
        };
        $.ajax(payload);
    };

    function handleProjectSelection(e) {
        var project = $("#google_project").val();
        if (project == "placeholder") {
            return;
        }

        var cred = getCredentials($("#google_credentials_name").val());

        var payload = {
            type: "GET",
            async: true,
            url: "/custom/Splunk_TA_google/proxy/servicesNS/admin/Splunk_TA_google/splunk_ta_google/google_subscriptions?output_mode=json&count=-1&google_project=" + project + "&google_credentials=" + cred,
            success: function(data) {
                if (data.entry === undefined || data.entry.length == 0) {
                    // FIXME
                    return false;
                }
                $.each(data.entry[0].content.subscriptions, function(key, value) {
                     $("#google_subscription").append(
                         $("<option></option>")
                         .attr("value", value)
                         .text(value));
                });
                enable("#google_subscription");
            },
            error: function(data) {
                // FIXME
                console.log(data);
            }
        };
        $.ajax(payload);
    };

    function populateCredentialDropdowns(column) {
        var options = '<option value="placeholder">--- Select Google Credentials ---</option>';
        var data = tables.credTable.data;
        for (var i = 0; i < data.length; i++) {
            options += '<option value="' + data[i][0] + '">' + data[i][0] + '</option>';
        }
        var dropdown_html = '<select name="' + column.name_with_desc + '" id="' + column.id + '" ' + column.required + '>' + options + '</select>';
        var input = $(dropdown_html);
        setTimeout(function() {
            $("#" + column.id).change(handleCredSelection);
        }, 1000);
        return input;
    };

    function disable(id) {
        $(id).prop("disabled", true);
        $(id).addClass("disabled");
    };

    function enable(id) {
        $(id).removeClass("disabled");
        $(id).prop("disabled", false);
    };

    function doPopulateDropdowns(column, msg) {
        var options = '<option value="placeholder">--- ' + msg + ' ---</option>';
        // var options = "";
        var dropdown_html = '<select name="' + column.name_with_desc + '" id="' + column.id + '" ' + column.required + '>' + options + '</select>';
        setTimeout(function() {
            disable("#" + column.id);
        }, 1000);

        return $(dropdown_html);
    };

    function populateGoogleProjectDropdowns(column) {
        var res = doPopulateDropdowns(column, "Select Google PubSub project");
        setTimeout(function() {
            $("#" + column.id).change(handleProjectSelection);
        }, 1000);
        return res;
    };

    function populateGoogleSubscriptionDropdowns(column) {
        return doPopulateDropdowns(column, "Select Google PubSub Subscription");
    };

    function enjectDialogForm(dialogId, formId, cols) {
        var form = $("#" + formId);
        cols.forEach(function(column){
            if (column.dialogHide) {
                return;
            }
            var container = $("<div></div>");
            var label = undefined;
            if (column.required == "required") {
                label = $("<label for='" + column.id + "'>" + column.name_with_desc + '<span class="requiredAsterisk"> *</span></label>');
            } else {
                label = $("<label for='" + column.id + "'>" + column.name_with_desc + "</label>");
            }
            var type = "text";
            if (column.id == "password") {
                type = "password";
            }

            // FIXME not generic code here
            var input = undefined;
            if (column.id == "google_credentials_name") {
                input = populateCredentialDropdowns(column);
            } else if (column.id == "google_project") {
                input = populateGoogleProjectDropdowns(column);
            } else if (column.id == "google_subscription") {
                input = populateGoogleSubscriptionDropdowns(column);
            }else {
                input = $("<input type='" + type + "' name='" + column.name_with_desc + "' id='" + column.id + "' " + column.required + "/>");
            }
            container.append(label);
            container.append(input);
            container.append($("</br>"));
            form.append(container);
            form.append("<br><br>");
        });
        var container = $('<div style="display: inline;"></div>');
        var saveBtnId = dialogId + "BtnSave";
        var cancelBtnId = dialogId + "BtnCancel";
        container.append($("<button id='" + saveBtnId + "' type='submit'><span>Save</span></button>"));
        container.append($("<button id='" + cancelBtnId + "' <span>Cancel</span></button>"));
        form.append(container);
        $("#" + cancelBtnId).click(hideDialogHandler);
        $("#" + saveBtnId).click(verifyDialogHandler);
    };

    function submitHandler(event) {
        var formId = event.target.id;
        submitForm(formId);
        event.preventDefault();
    }

    function submitForm(formId) {
        var formIdToDialog = {
            "credForm": dialogs.credDialog,
            "dataCollectionForm": dialogs.dataCollectionDialog,
        }

        var dialog = formIdToDialog[formId];
        var label = $("label[for='" + dialog.table.columns[0].id + "']");

        // label.text(dialog.table.columns[0].name + ": ");
        // label.css("color", "black");

        var row = [];
        dialog.table.columns.forEach(function(c, i){
            row[i] = $("#" + c.id).val();
        });

        if (row[0] in dialog.table.dataMap && currentAction == "New") {
            label.text(dialog.table.columns[0].name + ": " + row[0] + " already exists");
            label.css("color", "red");
            return;
        }

        if (currentAction == "Edit") {
            for (var i = 0; i < dialog.table.data.length; i++) {
                if (dialog.table.data[i][0] == row[0]) {
                    dialog.table.data[i] = row;
                    break;
                }
            }
        } else {
            dialog.table.data.push(row);
        }
        dialog.table.dataMap[row[0]] = row;
        updateTable(dialog.table.id, dialog.table.data, dialog.table.columns);
        hideDialog(dialog.id);
        clearFlag();
    }

    setTimeout(function() {
        for (var dialogId in dialogs) {
            $("#" + dialogs[dialogId].formId).submit(submitHandler);
            $("#" + dialogs[dialogId].formId + " input").off("keypress").keypress(dialogId, function(e) {
                if (e.which == 13) {
                    $("#" + e.data + "BtnSave").click();
                    return false;
                }
            });
        }
    }, 3000);

    $("#enable_proxy_id").on("change", showHideProxy);
    showHideProxy();

    $(".splButton-primary").on("click", function(){
        var jsonResult = JSON.stringify(getJSONResult());
        $(allSettingsEboxId).val(jsonResult);
        // console.log(jsonResult);

        /*var frm = $("#eaiform");
        var payload = {
            type: frm.attr("method"),
            async: true, // FIXME
            url: frm.attr("action"),
            data: frm.serialize(),
            success: function(data) {
                data = jQuery.parseJSON(data);
                if (data["status"] == "ERROR") {
                    // FIXME to show on GUI
                    var msg = $('<li class="message info"><div style="float:left">Successfully updated "Splunk_TA_google". </div><div style="clear:both"></div></li>');
                    $(".MessageList").append(msg);
                    console.log("Failed");
                }
            },
            error: function(data) {
                console.log("Failed");
            }
        };
        $.ajax(payload);
        return false;*/
    });
})
