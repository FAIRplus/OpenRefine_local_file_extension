ExporterManager.MenuItems.push({});//add separator


ExporterManager.MenuItems.push(
    {
        "id": "exportWorkspaceData",
        "label": 'CSV to Workspace Data',
        "click": function () {
            WorkspaceDataExporterMenuBar.exportCSV("csv", "csv");
        }
    }
);

WorkspaceDataExporterMenuBar = {};

WorkspaceDataExporterMenuBar.exportCSV = function (format, ext) {
    var name = encodeURI(ExporterManager.stripNonFileChars(theProject.metadata.name));

    $.ajax({
        url: 'command/local-file-system/workspace-export?' + $.param({
            project: theProject.id,
            name: name,
            format: format,
            quoteAll: true,
            engine: JSON.stringify(ui.browsingEngine.getJSON())
        }),
        type: "POST",
        success: function (response) {
            alert(response.message)
        },
    });
};


