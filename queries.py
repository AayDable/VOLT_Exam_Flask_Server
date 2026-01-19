pgsql_queries = \
{

"retrieve_dashboard_data":

"""
select c2."rollNo", c2."candidateName", eh."hallName", c."courseName", 
        pm."examDate", 
        CASE 
          WHEN ec."candidateExamStartTime" is null THEN NULL
          ELSE t."TotalCandidateScore"  
        END as "TotalCandidateScore",
        CASE 
          WHEN ec."candidateExamStartTime" is null THEN NULL
          ELSE t."MaxPossibleScore"  
        END as "MaxPossibleScore",
        CASE 
          WHEN ec."candidateExamStartTime" is null THEN NULL
          ELSE t."ScorePercentage"  
        END as "ScorePercentage"
from (
    select rs."projectMasterId", rs."candidateId", 
            sum(rs."score") as "TotalCandidateScore", 
            sum(rs."totalScore") as "MaxPossibleScore", 
            sum(rs."score")/sum(rs."totalScore") * 100 as "ScorePercentage"
    from "rawScore" rs
    where rs."isDeleted" = false
    group by rs."projectMasterId", rs."candidateId"
) as t
left outer join "projectMaster" pm on pm.id = t."projectMasterId"
left outer join "course" c on c.id = pm."courseId"
left outer join "candidate" c2 on c2.id = t."candidateId"
left outer join "examHall" eh on eh.id = c2."examHallId"
left outer join "examConfig" ec on ec."candidateId" = c2."id"
where pm."isGeneratingScores" = false
and pm."isDeleted" = false
and pm."enabledBy" > 0
and c2."isDeleted" = false
and c2."rPacksUploaded" = false
and c."isDeleted" = false;
"""
,

"""retrieve_all_data""":

"""select * from public.'{{tn}}'"""
}