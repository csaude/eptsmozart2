<%@ include file="/WEB-INF/template/include.jsp"%>

<openmrs:require privilege="Generate MozART2" otherwise="/login.htm" />

<%@ include file="/WEB-INF/template/header.jsp"%>

<h2><spring:message code="eptsmozart2.title" /></h2>
<openmrs:message var="pageTitle" code="eptsmozart2.title" scope="page"/>
<br/>

<script type="text/javascript">
    var localOpenmrsContextPath = '${pageContext.request.contextPath}';
    var progressUpdateSchedule = null;

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

    function cancelMozart2Generation() {
        if(progressUpdateSchedule) {
            clearTimeout(progressUpdateSchedule);
        }
        $j('#mozart2-cancel-button').prop('disabled', true);
        var requestHeaders = new Headers({
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        });

        var requestOptions = {
            method: 'GET',
            headers: requestHeaders,
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
                console.log('Returned data:', data);
                $j('#progress-table').css('visibility', 'visible');
                data.forEach(tableEntry => {
                    tableProgress(tableEntry);
                });
                $j('#mozart2-button').prop('disabled', false);
                $j('#mozart2-cancel-button').css('visibility', 'hidden');
            }).catch(error => {
                console.log(error);
            });
    }

    function requestMozart2Generation() {
        $j('#mozart2-button').prop('disabled', true);
        $j('#mozart2-cancel-button').css('visibility', 'visible');
        var requestHeaders = new Headers({
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        });

        var requestOptions = {
            method: 'POST',
            headers: requestHeaders,
        };

        fetch(localOpenmrsContextPath + "/module/eptsmozart2/eptsmozart2.json", requestOptions)
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
                    if(tableEntry.toBeGenerated !== tableEntry.generated) {
                        continueCheckingProgress = true;
                    }
                    tableProgress(tableEntry);
                });
                if(continueCheckingProgress) {
                    progressUpdateSchedule = setTimeout(requestMozart2Generation, 10000);
                } else {
                    $j('#mozart2-button').prop('disabled', false);
                }
            }).catch(error => {
                console.log(error);
            });
    }

    $j(document).ready(function() {
        <c:forEach items="${statuses}" var="tableEntry">
            $j('#${tableEntry.table}-progress-id').progressbar({
                value: 0,
            });
        </c:forEach>
    });
</script>
<button id = "mozart2-button" onclick="requestMozart2Generation()">Generate Mozart2</button>
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
    <button id = "mozart2-cancel-button" onclick="cancelMozart2Generation()" style="visibility: hidden;">Cancel Mozart2</button>
</div>
<%@ include file="/WEB-INF/template/footer.jsp"%>
