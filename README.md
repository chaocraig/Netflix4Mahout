Netflix4Mahout
==============

Translate Netflix dataset into Mahout dataset


 * Author:  Craig Kuo-Jen Chao
 * Date: 2014/04/17
 * Topic: Translate the Netflix Dataset into Mahout CF input dataset
 * Description: 
 *     Nextfile dataset format: 
 *          A. file names:      mv_0000001.txt ~ mv_0014810.txt
 *          B. content format:  
 *                              movie_id:
 *                              userid1,rate,date
 *                              userid2,rate,date
 *                              userid3,rate,date       
 *                              .....   
 *     Translated Mahout CF input dataset
 *          A. file names:      netflix4mahout.csv
 *          B. content format:
 *                              user,item,rate
 *                              ....
 

