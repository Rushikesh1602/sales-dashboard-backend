const express = require('express');
const mysql = require('mysql2');
const cors = require('cors');
const fs = require('fs');
const { Client } = require('ssh2');
const path = require('path');
const fsPromises = require('fs').promises;
const cron = require('node-cron');

const app = express();
const port = 3009;

app.use(cors());
app.use(express.json());
app.use(express.static(path.join(__dirname, 'frontend/build'))); // Serve React static files

// SSH Configuration
const sshConfig = {
    host: '10.10.0.195',
    port: 22,
    username: 'ubuntu',
    privateKey: fs.readFileSync('C:/Users/user/Downloads/hd_private_prod2.pem'),
    passphrase: 'hda+X53aZdRW3F$#'
};

// MySQL Configuration
const mysqlConfig = {
    host: '10.10.0.52',
    user: 'root',
    password: 'hda+X53aZdRW3F$#',
    database: 'haqdarsh_hd',
    port: 3306
};

// Define the correct path to the public folder
const publicDir = path.join('D:', 'Sales Demo', 'frontend', 'public');

// Establish SSH Tunnel
const ssh = new Client();
ssh.on('ready', () => {
    console.log('SSH Connection established.');

    ssh.forwardOut(
        '10.10.0.195',
        3306,
        mysqlConfig.host,
        mysqlConfig.port,
        async (err, stream) => {
            if (err) throw err;

            mysqlConfig.stream = stream;
            const connection = mysql.createConnection(mysqlConfig);

            connection.connect((err) => {
                if (err) throw err;
                console.log('Database connection established.');
            });

            // Fetch and store data function
            const fetchAndStoreData = async () => {
                try {
                    const [applications] = await connection.promise().query(`
                        SELECT 
                            SUM(CASE 
                                WHEN (timeline_summary.DATA = 'Open' OR timeline_summary.DATA = 'Data complete') 
                                THEN timeline_summary.cnt_timeline 
                                ELSE 0 
                            END) AS opencount
                        FROM 
                            timeline_summary;
                    `);

                    const [citizenImpacted] = await connection.promise().query(`
                        SELECT COUNT(DISTINCT p.id) AS 'Unique Citizens Impacted'
                        FROM episodes e
                        INNER JOIN person p ON p.id = e.person_id
                        WHERE
                            e.status IN (8)
                            AND e.organization_id NOT IN (1,212,278,279,280,281,282,359,360,361,362,363,364,365,366,367,368,369,370,371,372,373,374,375,376,377,378,421,443,450,454,455,622,623,677,866,877,1087);
                    `);

                    const [msmesImpacted] = await connection.promise().query(`
                        SELECT COUNT(DISTINCT enterprise.reference_id) AS 'Number of total MSME reached/screened'
                        FROM enterprise
                        WHERE
                            enterprise.organization_id NOT IN (1,212,278,279,280,281,282,359,360,361,362,363,364,365,366,367,368,369,370,371,372,373,374,375,376,377,378,421,443,450,454,455,677,743,857);
                    `);

                    const [hdsTrained] = await connection.promise().query(`
                        SELECT COUNT(users.id) AS 'Number of Haqdarshaks Trained'
                        FROM users
                        WHERE
                            users.role_id = '6'
                            AND users.organization_id NOT IN (1,212,278,279,280,281,282,359,360,361,362,363,364,365,366,367,368,369,370,371,372,373,374,375,376,377,378,421,443,450,454,455,622,623,677,866,877,1087);
                    `);

                    const [benefitsUnblocked] = await connection.promise().query(`
                        SELECT 
                            SUM(t1.value * t1.receivedcnt) AS total_bv
                        FROM (
                            SELECT 
                                s.value AS value,
                                SUM(CASE WHEN ts.data = "Scheme/Document received" THEN ts.cnt_timeline ELSE 0 END) AS receivedcnt
                            FROM 
                                timeline_summary ts
                            JOIN 
                                schemes s ON s.guid = ts.chief_concept 
                            GROUP BY 
                                s.value
                        ) AS t1;
                    `);

                    // New query to add
                    const [projectData] = await connection.promise().query(`
                    SELECT
                    os.value AS st_nm,
                    o.id AS ORG_ID,
                    o.name AS ORG_NAME,
                    o.pid AS PROJECT_ID,
                    pm.project_name AS Project_Name,
                    pm.project_type AS Project_Type,
                    pm.project_status AS Project_Status,
                    s.guid AS scheme_id,
                    s.status AS Scheme_Status,
                    s.type AS 'Type(Sch/Doc)',
                    CASE
                        WHEN s.target_beneficiary = '0' THEN 'Individual'
                        WHEN s.target_beneficiary = '1' THEN 'MSME'
                        WHEN s.target_beneficiary = '2' THEN 'Both'
                        ELSE NULL
                    END AS 'Individual/MSME',
                    sl.name AS scheme_name,
                    s.fee AS Scheme_fees,
                    s.type AS Scheme_type,
                    s.value AS BVl,
                    SUM(s.value) AS BV,
                    COUNT(CASE WHEN e.status <> 4 THEN 1 END) AS OpenCount,
                    COUNT(CASE WHEN e.status IN ('8', '7', '9') THEN 1 END) AS submitted,
                    COUNT(CASE WHEN e.status = '8' THEN 1 END) AS BR,
                    COALESCE(ut.Number_of_Haqdarshaks_Trained, 0) AS 'Number_of_Haqdarshaks_Trained',
                    COALESCE(mr.Number_of_total_msme_reached, 0) AS 'Number_of_total_msme_reached/screened',
                    COALESCE(citizen.Citizen_impacted, 0) AS 'Citizen_impacted'
                FROM
                    organizations o
                JOIN organization_settings os ON os.organization_id = o.id AND os.key = 'state'
                JOIN episodes e ON e.organization_id = o.id
                JOIN schemes s ON s.guid = e.chief_concept
                JOIN schemes_langs sl ON sl.scheme_id = s.id AND sl.lang = 'en'
                INNER JOIN project_management pm ON o.pid = pm.pid
                LEFT JOIN (
                    -- Subquery to get the count of trained Haqdarshaks by state
                    SELECT  
                        os.value AS st_nm,
                        COUNT(users.id) AS Number_of_Haqdarshaks_Trained
                    FROM users
                    JOIN organizations o ON o.id = users.organization_id
                    JOIN organization_settings os ON os.organization_id = o.id AND os.key = 'state'
                    WHERE
                        users.role_id = '6'
                        AND users.organization_id NOT IN (1, 212, 278, 279, 280, 281, 282, 359, 360, 361, 362, 363, 364, 365, 366, 367, 368, 369, 370, 371, 372, 373, 374, 375, 376, 377, 378, 421, 443, 450, 454, 455, 622, 623, 677, 866, 877, 1087)
                    GROUP BY os.value
                ) AS ut ON os.value = ut.st_nm
                LEFT JOIN (
                    -- Subquery to get the count of MSME reached/screened by state
                    SELECT  
                        os.value AS st_nm,
                        COUNT(DISTINCT enterprise.reference_id) AS Number_of_total_msme_reached
                    FROM enterprise
                    JOIN organizations o ON o.id = enterprise.organization_id
                    JOIN organization_settings os ON os.organization_id = o.id AND os.key = 'state'
                    WHERE
                        enterprise.organization_id NOT IN (1, 212, 278, 279, 280, 281, 282, 359, 360, 361, 362, 363, 364, 365, 366, 367, 368, 369, 370, 371, 372, 373, 374, 375, 376, 377, 378, 421, 443, 450, 454, 455, 677, 743, 857)
                    GROUP BY os.value
                ) AS mr ON os.value = mr.st_nm
                LEFT JOIN (
                    -- Subquery to get the count of impacted citizens by state
                    SELECT  
                        os.value AS st_nm,
                        COUNT(p.id) AS Citizen_impacted
                    FROM person p
                    JOIN organizations o ON o.id = p.organization_id
                    JOIN organization_settings os ON os.organization_id = o.id AND os.key = 'state'
                    WHERE
                        p.organization_id NOT IN (1, 212, 278, 279, 280, 281, 282, 359, 360, 361, 362, 363, 364, 365, 366, 367, 368, 369, 370, 371, 372, 373, 374, 375, 376, 377, 378, 421, 443, 450, 454, 455, 677, 743, 857)
                    GROUP BY os.value
                ) AS citizen ON os.value = citizen.st_nm
                WHERE
                    o.id NOT IN (1, 212, 278, 279, 280, 281, 282, 359, 360, 361, 362, 363, 364, 365, 366, 367, 368, 369, 370, 371, 372, 373, 374, 375, 376, 377, 378, 421, 443, 450, 454, 455, 622, 623, 677, 866, 877, 1087)
                    AND pm.project_status IN ('In Progress', 'Completed', 'Closed')
                GROUP BY
                    o.pid, e.chief_concept, pm.project_name, pm.project_type, pm.project_status, os.value
                ORDER BY
                    pm.project_type;
                    `);

                    // Data to save
                    const dataToSave = {
                        applications_submitted: applications[0]['opencount'],
                        citizen_impacted: citizenImpacted[0]['Unique Citizens Impacted'],
                        msmes_impacted: msmesImpacted[0]['Number of total MSME reached/screened'],
                        hds_trained: hdsTrained[0]['Number of Haqdarshaks Trained'],
                        benefits_unblocked: benefitsUnblocked[0]['total_bv']
                    };

                    const projectDataToSave = projectData;

                    // Save data to the public folder
                    await fsPromises.writeFile(path.join(publicDir, 'data.json'), JSON.stringify(dataToSave, null, 2));
                    await fsPromises.writeFile(path.join(publicDir, 'project_data.json'), JSON.stringify(projectDataToSave, null, 2));
                    console.log('Data successfully saved to public/data.json and public/project_data.json');
                } catch (err) {
                    console.error('Error fetching and saving data:', err);
                }
            };

            // Schedule the fetchAndStoreData function to run at 12am every day
            cron.schedule('0 0 * * *', () => {
                console.log('Running scheduled data fetch and store at 12am');
                fetchAndStoreData();
            });

            // Fetch data on server start
            fetchAndStoreData();

            app.listen(port, () => {
                console.log(`Server running on port ${port}`);
            });
        }
    );
}).connect(sshConfig);

ssh.on('error', (err) => {
    console.error('SSH Connection error:', err);
});
