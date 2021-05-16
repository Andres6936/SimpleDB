package simpledb.server;

import java.io.File;

import simpledb.file.FileMgr;
import simpledb.log.LogMgr;
import simpledb.buffer.BufferMgr;
import simpledb.tx.Transaction;
import simpledb.metadata.MetadataMgr;
import simpledb.plan.*;
import simpledb.index.planner.IndexUpdatePlanner;
import simpledb.opt.HeuristicQueryPlanner;

/**
 * The class that configures the system. <br/><br/>
 * <p>
 * A query in SimpleDB consists only of select-from-where clauses in which the
 * select clause contains a list of field names (without the AS keyword), and
 * the from clause contains a list of table names (without range variables).
 * The terms in the optional where clause can be connected only by the boolean
 * operator and. Terms can only compare constants and field names for equality.
 * <br/><br/>
 * <p>
 * Unlike standard SQL, there are no other comparison operators, no other
 * boolean operators, no arithmetic operators or built-in functions, and no
 * parentheses. Consequently, nested queries, aggregation, and computed values
 * are not supported. <br/><br/>
 * <p>
 * Because there are no range variables and no renaming, all field names in a
 * query must be disjoint. And because there are no group by or order by
 * clauses, grouping and sorting are not supported. Other restrictions are:
 * <br/><br/>
 *
 * <ul>
 *     <li>The “*” abbreviation in the select clause is not supported.</li>
 *     <li>There are no null values.</li>
 *     <li>There are no explicit joins or outer joins in the from clause.</li>
 *     <li>The union keyword is not supported.</li>
 *     <li>An insert statement takes explicit values only. That is, an insertion
 *     cannot be specified by a query.</li>
 *     <li>An update statement can have only one assignment in the set clause.</li>
 * </ul>
 *
 * @author Edward Sciore
 */
public class SimpleDB {
    public static int BLOCK_SIZE = 400;
    public static int BUFFER_SIZE = 8;
    public static String LOG_FILE = "simpledb.log";

    private FileMgr fm;
    private BufferMgr bm;
    private LogMgr lm;
    private MetadataMgr mdm;
    private Planner planner;

    /**
     * A constructor useful for debugging.
     *
     * @param dirname   the name of the database directory
     * @param blocksize the block size
     * @param buffsize  the number of buffers
     */
    public SimpleDB(String dirname, int blocksize, int buffsize) {
        File dbDirectory = new File(dirname);
        fm = new FileMgr(dbDirectory, blocksize);
        lm = new LogMgr(fm, LOG_FILE);
        bm = new BufferMgr(fm, lm, buffsize);
    }

    /**
     * A simpler constructor for most situations. Unlike the
     * 3-arg constructor, it also initializes the metadata tables.
     *
     * @param dirname the name of the database directory
     */
    public SimpleDB(String dirname) {
        this(dirname, BLOCK_SIZE, BUFFER_SIZE);
        Transaction tx = newTx();
        boolean isnew = fm.isNew();
        if (isnew)
            System.out.println("creating new database");
        else {
            System.out.println("recovering existing database");
            tx.recover();
        }
        mdm = new MetadataMgr(isnew, tx);
        QueryPlanner qp = new BasicQueryPlanner(mdm);
        UpdatePlanner up = new BasicUpdatePlanner(mdm);
//    QueryPlanner qp = new HeuristicQueryPlanner(mdm);
//    UpdatePlanner up = new IndexUpdatePlanner(mdm);
        planner = new Planner(qp, up);
        tx.commit();
    }

    /**
     * A convenient way for clients to create transactions
     * and access the metadata.
     */
    public Transaction newTx() {
        return new Transaction(fm, lm, bm);
    }

    public MetadataMgr mdMgr() {
        return mdm;
    }

    public Planner planner() {
        return planner;
    }

    // These methods aid in debugging
    public FileMgr fileMgr() {
        return fm;
    }

    public LogMgr logMgr() {
        return lm;
    }

    public BufferMgr bufferMgr() {
        return bm;
    }
}
