<%@ include file="/WEB-INF/template/include.jsp"%>

<openmrs:require privilege="Generate MozART2" otherwise="/login.htm" />

<%@ include file="/WEB-INF/template/header.jsp"%>

<h2><spring:message code="eptsmozart2.title" /></h2>
<openmrs:message var="pageTitle" code="eptsmozart2.title" scope="page"/>
<br/>

<script type="text/javascript">
    var localOpenmrsContextPath = '${pageContext.request.contextPath}';
    var progressUpdateSchedule = null;
    var TIME_INTERVAL_BETWEEN_STATUS_CHECK = 20000;
    var tableProgressBarMap = {};

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
                data.forEach(tableEntry => {
                    // Reset progress bar
                    $j('#' + tableEntry.table + '-progress-id').progressbar({ value: 0});
                });
                $j('#mozart2-button').prop('disabled', false);
                $j('#mozart2-cancel-button').prop('disabled', false);
                $j('#mozart2-cancel-button').css('visibility', 'hidden');
            }).catch(error => {
                console.log(error);
            });
    }

    function requestMozart2Generation() {
        $j('#mozart2-button').prop('disabled', true);
        $j('#mozart2-cancel-button').css('visibility', 'visible');

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
                data.forEach(tableEntry => {
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
                    $j('#mozart2-cancel-button').css('visibility', 'hidden');
                }
            }).catch(error => {
                console.log(error);
            });
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

    function initialStatusRequest() {
        $j('#mozart2-button').prop('disabled', true);
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
                data.forEach(tableEntry => {
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
                }

                if(continueCheckingProgress) {
                    $j('#mozart2-cancel-button').css('visibility', 'visible');
                    progressUpdateSchedule = setTimeout(requestStatusAndUpgradeProgress, TIME_INTERVAL_BETWEEN_STATUS_CHECK);
                } else {
                    if(progressUpdateSchedule) {
                        clearTimeout(progressUpdateSchedule);
                    }
                    $j('#mozart2-button').prop('disabled', false);
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
                data.forEach(tableEntry => {
                    if (tableEntry.toBeGenerated !== tableEntry.generated || tableEntry.generated === 0) {
                        continueCheckingProgress = true;
                    } else if(tableEntry.toBeGenerated === tableEntry.generated) {
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
                    $j('#mozart2-cancel-button').css('visibility', 'hidden');
                }
            }).catch(error => {
                console.log(error);
            });
    }

    $j(document).ready(function() {
        initialStatusRequest();
        
        $j('#end-date-picker').datepicker({
            changeMonth: true,
            changeYear: true,
            dateFormat: 'dd-mm-yy',
            maxDate: new Date()
        });

        $j('#end-date-picker').datepicker('setDate', new Date());
    });
</script>
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
    <button id = "mozart2-cancel-button" onclick="cancelMozart2Generation()" style="visibility: hidden;"><openmrs:message code="eptsmozart2.cancel.mozart2.button.label"/></button>
</div>
<%@ include file="/WEB-INF/template/footer.jsp"%>
