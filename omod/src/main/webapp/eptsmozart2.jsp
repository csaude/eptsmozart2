<%@ include file="/WEB-INF/template/include.jsp"%>

<openmrs:require privilege="Generate MozART2" otherwise="/login.htm" />

<%@ include file="/WEB-INF/template/header.jsp"%>

<h2><spring:message code="eptsmozart2.title" /></h2>
<openmrs:message var="pageTitle" code="eptsmozart2.title" scope="page"/>
<br/>

<openmrs:htmlInclude file="/scripts/jquery/dataTables/css/dataTables_jui.css"/>
<openmrs:htmlInclude file="/scripts/jquery/dataTables/js/jquery.dataTables.min.js" />

<style>
    div {
        margin-bottom: 1em;
    }

    table.vertical_table {
        margin-top: 10px;
    }

    table.vertical_table, table.vertical_table > * > tr > th, table.vertical_table > * > tr > td {
        border: 1px solid black;
        border-collapse: collapse;
        text-align: left;
        padding: 0.5em;
        width: 100%;
    }

    table.vertical_table > * > tr > th {
        width: fit-content;
        white-space:nowrap;
    }
</style>

<script type="text/javascript">
    var localOpenmrsContextPath = '${pageContext.request.contextPath}';
    var progressUpdateSchedule = null;
    var TIME_INTERVAL_BETWEEN_STATUS_CHECK = 20000;
    var tableProgressBarMap = {};
    var lastGeneration = <c:choose><c:when test="${not empty lastGeneration}">${lastGeneration.id}</c:when><c:otherwise>-1</c:otherwise></c:choose>;

    class HttpError extends Error {
        constructor(response) {
            super(`${response.status} for ${response.url}`);
            this.name = 'HttpError';
            this.response = response;
        }
    }

    function tableProgress(tableEntry) {
        $j('#' + tableEntry.table + '-to-generate-value').html(tableEntry.toBeGenerated);
        if(tableEntry.toBeGenerated > 0) {
            let newValue = Math.floor(tableEntry.generated / tableEntry.toBeGenerated * 100);
            $j('#' + tableEntry.table + '-progress-id').progressbar('value', newValue);
        }
    }

    function ProgressBarState(progressBarElement) {
        this.progressBarElement = progressBarElement;
        this.keepOn = true;
        this.pulsate = function() {
            var _that = this;
            this.progressBarElement.effect("pulsate", { times:1 }, 3000,function(){
                //repeat after pulsating
                if(_that.keepOn) {
                    _that.pulsate();
                }
            });
        };
    }

    function resetToBeGeneratedValuesAndProgressBars() {
        var tableNames = Object.keys(tableProgressBarMap);
        if(tableNames.length > 0) {
            tableNames.forEach(function(table) {
                $j('#' + table + '-to-generate-value').html(0);
                $j('#' + table + '-progress-id').progressbar('value', 0);
                tableProgressBarMap[table].pulsate();
            });
        }
    }

    function updateLastGenerationData(generation) {
        if(generation === null || generation === undefined) return;
        if((typeof lastGeneration !== 'object' && lastGeneration !== generation.id) || lastGeneration['id'] !== generation.id) {
            $j('#recent-generation-executor').html(generation.executor.fullname + ' (' + generation.executor.username + ')');
            $j('#recent-generation-date-started').html(generation.dateStarted);
        }
        if($j('#recent-generation-date-started').is(':empty')) {
            $j('#recent-generation-date-started').html(generation.dateStarted);
        }
        $j('#recent-generation-date-ended').html(generation.dateEnded);
        $j('#recent-generation-status').html(generation.status);
        if(generation.status == 'COMPLETED' && generation.sqlDumpFilename) {
            var anchorTag = '<a href="' + localOpenmrsContextPath + '/module/eptsmozart2/eptsmozart2download.json?id=' + generation.id + '">';
            anchorTag += '<openmrs:message code="eptsmozart2.download.mozart2.button.label"/>';
            anchorTag += '</a>';
            $j('#recent-generation-action').append(anchorTag);
        } else {
            $j('#recent-generation-action').html();
        }
        lastGeneration = generation;
    }

    function formatDateToISO(date) {
        var month = date.getMonth() + 1;
        if(month < 10) {
            month = '0' + month;
        }
        var day = date.getDate();
        if(day < 10) {
            day = '0' + day;
        }
        return date.getFullYear() + '-' + month + '-' + day;
    }

    function cancelMozart2Generation() {
        if(progressUpdateSchedule) {
            clearTimeout(progressUpdateSchedule);
        }
        $j('#mozart2-cancel-button').prop('disabled', true);

        var requestOptions = {
            method: 'GET',
            headers: new Headers({
                'Content-Type': 'application/json',
                'Accept': 'application/json'
            }),
        };

        fetch(localOpenmrsContextPath + "/module/eptsmozart2/eptsmozart2cancel.json", requestOptions)
            .then(response => {
                if(response.status !== 200) {
                    throw new HttpError(response);
                } else {
                    return response.json()
                }
            })
            .then(data => {
                console.log('Cancellation Returned data:', data);
                $j('#progress-table').css('visibility', 'hidden');
                updateLastGenerationData(data['lastGeneration']);
                data['statuses'].forEach(tableEntry => {
                    // Reset progress bar
                    $j('#' + tableEntry.table + '-progress-id').progressbar({ value: 0});
                });
                $j('#mozart2-button').prop('disabled', false);
                $j('#end-date-picker').prop('disabled', false);
                $j('#mozart2-cancel-button').prop('disabled', false);
                $j('#mozart2-cancel-button').css('visibility', 'hidden');
            }).catch(error => {
                console.log(error);
            });
    }

    function requestMozart2Generation() {
        $j('#mozart2-button').prop('disabled', true);
        $j('#end-date-picker').prop('disabled', true);
        $j('#mozart2-cancel-button').css('visibility', 'visible');
        resetToBeGeneratedValuesAndProgressBars();

        var requestOptions = {
            method: 'POST',
            headers: new Headers({
                'Content-Type': 'application/json',
                'Accept': 'application/json'
            })
        };
        var reqUrl = localOpenmrsContextPath + '/module/eptsmozart2/eptsmozart2.json?endDate='
                + formatDateToISO($j('#end-date-picker').datepicker('getDate'));

        fetch(reqUrl, requestOptions)
            .then(response => {
                if(response.status !== 200) {
                    throw new HttpError(response);
                } else {
                    return response.json()
                }
            })
            .then(data => {
                console.log('Returned data:', data);
                $j('#progress-table').css('visibility', 'visible');
                var continueCheckingProgress = false;
                if(data['lastGeneration']) {
                    updateLastGenerationData(data['lastGeneration']);
                    $j('#last-generation-info').css('display','block');
                }
                data['statuses'].forEach(tableEntry => {
                    if(tableEntry.toBeGenerated !== tableEntry.generated || tableEntry.generated === 0) {
                        continueCheckingProgress = true;
                    }
                    tableProgress(tableEntry);
                });
                if(continueCheckingProgress) {
                    progressUpdateSchedule = setTimeout(requestStatusAndUpgradeProgress, TIME_INTERVAL_BETWEEN_STATUS_CHECK);
                } else {
                    if(progressUpdateSchedule) {
                        clearTimeout(progressUpdateSchedule);
                    }
                    $j('#mozart2-button').prop('disabled', false);
                    $j('#end-date-picker').prop('disabled', false);
                    $j('#mozart2-cancel-button').css('visibility', 'hidden');
                }
            }).catch(error => {
                console.log(error);
            });
    }

    function initialStatusRequest() {
        $j('#mozart2-button').prop('disabled', true);
        $j('#end-date-picker').prop('disabled', true);
        $j('#mozart2-cancel-button').css('visibility', 'hidden');

        var requestOptions = {
            method: 'GET',
            headers: new Headers({
                'Content-Type': 'application/json',
                'Accept': 'application/json'
            }),
        };

        fetch(localOpenmrsContextPath + "/module/eptsmozart2/eptsmozart2status.json", requestOptions)
            .then(response => {
                if(response.status !== 200) {
                    throw new HttpError(response);
                } else {
                    return response.json()
                }
            })
            .then(data => {
                console.log('Initial status data:', data);
                var progressTableVisible = false;
                var continueCheckingProgress = false;

                if(data['lastGeneration']) {
                    updateLastGenerationData(data['lastGeneration']);
                    $j('#last-generation-info').css('display', 'block');
                }
                data['statuses'].forEach(tableEntry => {
                    if (tableEntry.generated > 0) {
                        progressTableVisible = true;
                    }
                    if (tableEntry.toBeGenerated !== tableEntry.generated) {
                        continueCheckingProgress = true;
                    }

                    var progressBarElement = $j('#' + tableEntry.table + '-progress-id');
                    if (tableEntry.toBeGenerated > 0) {
                        progressBarElement.progressbar({
                            value: Math.floor(tableEntry.generated / tableEntry.toBeGenerated * 100),
                        });
                    } else {
                        progressBarElement.progressbar({
                            value: 0,
                        });
                    }
                    tableProgressBarMap[tableEntry.table] = new ProgressBarState(progressBarElement);
                    tableProgressBarMap[tableEntry.table].pulsate();
                });

                if(progressTableVisible) {
                    $j('#progress-table').css('visibility', 'visible');
                } else {
                    $j('#mozart2-button').prop('disabled', false);
                    $j('#end-date-picker').prop('disabled', false);
                }

                if(continueCheckingProgress) {
                    $j('#mozart2-cancel-button').css('visibility', 'visible');
                    progressUpdateSchedule = setTimeout(requestStatusAndUpgradeProgress, TIME_INTERVAL_BETWEEN_STATUS_CHECK);
                } else {
                    if(progressUpdateSchedule) {
                        clearTimeout(progressUpdateSchedule);
                    }
                    $j('#mozart2-button').prop('disabled', false);
                    $j('#end-date-picker').prop('disabled', false);
                }
            }).catch(error => {
                console.log(error);
            });
    }

    function requestStatusAndUpgradeProgress() {
        var requestOptions = {
            method: 'GET',
            headers: new Headers({
                'Content-Type': 'application/json',
                'Accept': 'application/json'
            }),
        };

        fetch(localOpenmrsContextPath + "/module/eptsmozart2/eptsmozart2status.json", requestOptions)
            .then(response => {
                if(response.status !== 200) {
                    throw new HttpError(response);
                } else {
                    return response.json()
                }
            })
            .then(data => {
                console.log('Status data:', data);
                var continueCheckingProgress = false;
                updateLastGenerationData(data['lastGeneration']);
                data['statuses'].forEach(tableEntry => {
                    if (tableEntry.toBeGenerated !== tableEntry.generated || tableEntry.generated === 0) {
                        continueCheckingProgress = true;
                    } else if(tableEntry.toBeGenerated === tableEntry.generated && tableEntry.generated > 0) {
                        tableProgressBarMap[tableEntry.table].keepOn = false;
                    }
                    tableProgress(tableEntry);
                });

                if(continueCheckingProgress) {
                    progressUpdateSchedule = setTimeout(requestStatusAndUpgradeProgress, TIME_INTERVAL_BETWEEN_STATUS_CHECK);
                } else {
                    if(progressUpdateSchedule) {
                        clearTimeout(progressUpdateSchedule);
                    }
                    $j('#mozart2-button').prop('disabled', false);
                    $j('#end-date-picker').prop('disabled', false);
                    $j('#mozart2-cancel-button').css('visibility', 'hidden');
                }
            }).catch(error => {
                console.log(error);
            });
    }

    $j(document).ready(function() {
        initialStatusRequest();

        $j('#mozart2-tabs').tabs();

        $j('#generation-history-table').dataTable();

        $j('#end-date-picker').datepicker({
            changeMonth: true,
            changeYear: true,
            dateFormat: 'dd-mm-yy',
            maxDate: new Date()
        });

        $j('#end-date-picker').datepicker('setDate', new Date());
    });
</script>

<div id="last-generation-info" style="display:none;">
    <fieldset>
        <legend><openmrs:message code="eptsmozart2.most.recent.generation.label"/></legend>
        <table class="vertical_table">
            <tr>
                <th><openmrs:message code="eptsmozart2.generation.initiator.label"/></th>
                <td id="recent-generation-executor">
                    <c:if test="${not empty lastGeneration}">
                        <c:choose>
                            <c:when test="${not empty lastGeneration.executor}">
                                ${lastGeneration.executor.username}
                            </c:when>
                            <c:otherwise>
                                <openmrs:message code="eptsmozart2.generation.initiator.unknown.label"/>
                            </c:otherwise>
                        </c:choose>
                    </c:if>
                </td>
            </tr>
            <tr>
                <th><openmrs:message code="eptsmozart2.date.started.label"/></th>
                <td id="recent-generation-date-started">
                    <c:if test="${not empty lastGeneration}"></c:if>
                    ${lastGeneration.dateStarted}
                </td>
            </tr>
            <tr>
                <th><openmrs:message code="eptsmozart2.date.completed.label"/></th>
                <td id="recent-generation-date-ended">
                    <c:if test="${not empty lastGeneration}">${lastGeneration.dateEnded}</c:if>
                </td>
            </tr>
            <tr>
                <th><openmrs:message code="eptsmozart2.generation.status.label"/></th>
                <td id="recent-generation-status">
                    <c:if test="${not empty lastGeneration}">${lastGeneration.status}</c:if>
                </td>
            </tr>
                <th><openmrs:message code="eptsmozart2.action.label"/></th>
                <td id="recent-generation-action">
                    <c:if test="${not empty lastGeneration}">
                        <c:if test="${not empty generation.sqlDumpPath}">
                            <a href='${pageContext.request.contextPath.concat("/module/eptsmozart2/eptsmozart2download.json?id=").concat(lastGeneration.id)}'>
                                <openmrs:message code="eptsmozart2.download.mozart2.button.label"/>
                            </a>
                        </c:if>
                    </c:if>
                </td>
            </tr>
        </table>
    </fieldset>
</div>
<div id="mozart2-tabs">
    <ul>
        <li><a href="#mozart2-generation-tab"><openmrs:message code="eptsmozart2.generation.tab.label"/></a></li>

        <li><a href="#mozart2-history-tab"><openmrs:message code="eptsmozart2.history.tab.label"/></a></li>
    </ul>
    <div id="mozart2-generation-tab">
        <label for="end-date-picker"> <openmrs:message code="eptsmozart2.endDate.label"/></label>&nbsp;<input type="text" id="end-date-picker" name="endDate"/>
        <button id = "mozart2-button" onclick="requestMozart2Generation()" class="button"><openmrs:message code="eptsmozart2.generate.mozart2.button.label"/></button>
        <br/>
        <div id="progress-table" style = "visibility: hidden;">
            <table cellpadding="5" border="0" cellspacing="5" width="80%">
                <thead>
                <tr><th>Table</th><th>Records to Generate</th><th style="width: 70%">Progress</th></tr>
                </thead>
                <tbody>
                <c:forEach items="${statuses}" var="tableEntry">
                    <tr>
                        <td>${tableEntry.table}</td>
                        <td style="text-align:center;" id="${tableEntry.table}-to-generate-value">${tableEntry.toBeGenerated}</td>
                        <td><div id="${tableEntry.table}-progress-id"></div></td>
                    </tr>
                </c:forEach>
                </tbody>
            </table>
        </div>
        <div>
            <button id = "mozart2-cancel-button" onclick="cancelMozart2Generation()" style="visibility: hidden;">
                <openmrs:message code="eptsmozart2.cancel.mozart2.button.label"/>
            </button>
        </div>
    </div>
    <div id="mozart2-history-tab">
        <table id="generation-history-table" width="100%">
            <thead>
                <tr>
                    <th>S/N</th>
                    <th><openmrs:message code="eptsmozart2.generation.initiator.label"/></th>
                    <th><openmrs:message code="eptsmozart2.date.started.label"/></th>
                    <th><openmrs:message code="eptsmozart2.date.completed.label"/></th>
                    <th><openmrs:message code="eptsmozart2.generation.status.label"/></th>
                    <th><openmrs:message code="eptsmozart2.action.label"/></th>
                </tr>
            </thead>
            <tbody>
                <c:forEach items="${generations}" var="generation" varStatus="loop">
                    <tr>
                        <td>${loop.count}</td>
                        <td>
                            <c:choose>
                                <c:when test="${not empty generation.executor}">
                                    <c:if test="${not empty generation.executor.personName}">
                                        ${generation.executor.personName.fullName}
                                    </c:if>
                                    (${generation.executor.username})
                                </c:when>
                                <c:otherwise>
                                    <openmrs:message code="eptsmozart2.generation.initiator.unknown.label"/>
                                </c:otherwise>
                            </c:choose>
                        </td>
                        <td>${generation.dateStarted}</td>
                        <td>${generation.dateEnded}</td>
                        <td>${generation.status}</td>
                        <td>
                            <c:if test="${not empty generation.sqlDumpPath}">
                                <a href='${pageContext.request.contextPath.concat("/module/eptsmozart2/eptsmozart2download.json?id=").concat(generation.id)}'>
                                    <openmrs:message code="eptsmozart2.download.mozart2.button.label"/>
                                </a>
                            </c:if>
                        </td>
                    </tr>
                </c:forEach>
            </tbody>
        </table>
    </div>
</div>


<%@ include file="/WEB-INF/template/footer.jsp"%>
