--Ex 1.
-- Question 1
-- Write and execute a SQL query to list the school names, community names and average attendance for communities with a hardship index of 98.

SELECT CPS.NAME_OF_SCHOOL, CSD.COMMUNITY_AREA_NAME, CPS.AVERAGE_STUDENT_ATTENDANCE
FROM chicago_public_schools CPS
LEFT JOIN chicago_socioeconomic_data CSD
ON CPS.COMMUNITY_AREA_NUMBER = CSD.COMMUNITY_AREA_NUMBER
WHERE CSD.HARDSHIP_INDEX = 98;

-- Question 2
-- Write and execute a SQL query to list all crimes that took place at a school. Include case number, crime type and community name.
SELECT CC.CASE_NUMBER, CC.PRIMARY_TYPE, CSD.COMMUNITY_AREA_NAME
FROM chicago_crime CC
LEFT JOIN chicago_socioeconomic_data CSD ON CC.COMMUNITY_AREA_NUMBER = CSD.COMMUNITY_AREA_NUMBER
WHERE CC.LOCATION_DESCRIPTION LIKE 'SCHOOL%'



-- Ex 2: Create a View function
-- Q1.1
CREATE VIEW FROM_CPS AS
SELECT "NAME_OF_SCHOOL" AS School_Name,
				"Safety_Icon" AS Safety_Rating,
				"Family_Involvement_Icon" AS Family_Rating,
				"Environment_Icon" AS Environment_Rating,
				"Instruction_Icon" AS Instruction_Rating,
				"Leaders_Icon" AS Leaders_Rating,
				"Teachers_Icon" AS Teachers_Rating
FROM chicago_public_schools;
-- Q1.2

SELECT School_Name, Leaders_Rating FROM SCHOOL_ICONS

-- Ex 3: Creating a Stored Procedure

DELIMITER //
CREATE   PROCEDURE `UPDATE_LEADERS_SCORE` (IN in_School_ID  int,IN in_Leader_Score  int)
    BEGIN
        UPDATE chicago_public_schools
        SET Leaders_Score = in_Leader_Score
        WHERE School_ID = in_School_ID ;

        IF in_Leader_Score >0 AND in_Leader_Score <20
        THEN UPDATE chicago_public_schools
        SET Leaders_Icon ='Very Weak'
        WHERE School_ID = in_School_ID;
        ELSEIF in_Leader_Score < 40
        THEN UPDATE chicago_public_schools
        SET Leaders_Icon ='Weak'
        WHERE School_ID = in_School_ID;
        ELSEIF in_Leader_Score < 60
        THEN UPDATE chicago_public_schools
        SET Leaders_Icon ='Average'
        WHERE School_ID = in_School_ID;
        ELSEIF in_Leader_Score < 80
        THEN UPDATE chicago_public_schools
        SET Leaders_Icon ='Strong'
        WHERE School_ID = in_School_ID;
        ELSEIF in_Leader_Score < 100
        THEN UPDATE chicago_public_schools
        SET Leaders_Icon ='Very Strong'
        WHERE School_ID = in_School_ID;
    
        END IF;
END //

-- Ex 4
CREATE   PROCEDURE `UPDATE_LEADERS_SCORE` (IN in_School_ID  int,IN in_Leader_Score  int)
    BEGIN

    IF in_Leader_Score >= 0 AND in_Leader_Score <= 19 THEN
    UPDATE chicago_public_schools
    SET Leaders_Icon = "Very weak"
    WHERE School_ID = in_School_ID ;

    ELSEIF  in_Leader_Score <= 39 THEN
    UPDATE chicago_public_schools
    SET Leaders_Icon = "Weak"
    WHERE School_ID = in_School_ID ;

    ELSEIF in_Leader_Score <= 59 THEN
    UPDATE chicago_public_schools
    SET Leaders_Icon = "Average"
    WHERE School_ID = in_School_ID ;

    ELSEIF in_Leader_Score <= 79 THEN
    UPDATE chicago_public_schools
    SET Leaders_Icon = "Strong"
    WHERE School_ID = in_School_ID ;

    ELSEIF in_Leader_Score <= 99 THEN
    UPDATE chicago_public_schools
    SET Leaders_Icon = "Very strong"
    WHERE School_ID = in_School_ID ;
    ELSE ROLLBACK;
    END IF;
    COMMIT;
END
