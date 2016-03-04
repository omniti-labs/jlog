import com.omniti.labs.jlog;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;


public class jlogTest {
    static final String payload = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
    static final String[] names = { "00000001", "00000003", "0000010a", "cp.7473" } ;
    static final int pcnt = 1000000;
    String jlogpath = "/tmp/foo.jlog";
    //String[] args;

    void log(String m) { System.err.println(m); }

    void delete(File f) throws IOException {
        if (f.isDirectory()) {
            for (File c : f.listFiles())
                delete(c);
        }
        if (!f.delete())
            throw new FileNotFoundException("Failed to delete file: " + f);
    }

    void fail(String msg) throws Exception {
        throw new Exception(msg);
    }

    void test_subscriber(String s) throws Exception {
        log("adding subscriber " + s);
        jlog ctx = new jlog(jlogpath);
        ctx.add_subscriber(s, jlog.jlog_position.JLOG_BEGIN);
        try {
            ctx.add_subscriber(s, jlog.jlog_position.JLOG_BEGIN);
        } catch(jlog.jlogSubscriberExistsException ok) {
        }
    }

    void initialize() throws Exception {
        File f = new File(jlogpath);
        if(f.exists()) {
            log("cleaning up " + jlogpath);
            delete(f);
        }
        log("initializing new jlog at " + jlogpath);
        jlog ctx = new jlog(jlogpath);
        ctx.init();
    }

    void assert_subscriber(String s) throws Exception {
        log("checking subscriber " + s);
        jlog ctx = new jlog(jlogpath);
        String[] subs = ctx.list_subscribers();
        for(String sub : subs) {
            if(sub.equals(s)) return;
        }
        fail("nope");
    }

    jlog open_writer() throws Exception {
        log("opening writer");
        jlog ctx = new jlog(jlogpath);
        ctx.open_writer();
        return ctx;
    }

    jlog open_reader(String s) throws Exception {
        log("opening reader: " + s);
        jlog ctx = new jlog(jlogpath);
        ctx.open_reader(s);
        return ctx;
    }

    void write_payloads(int cnt) throws Exception {
        jlog ctx = open_writer();
        log("writing out " + cnt + " " + payload.getBytes().length + " byte payloads");
        for(int i = 0; i < cnt; i++)
            ctx.write(payload);
    }

    void read_check(String s, int expect, boolean sizeup) throws Exception {
        int cnt = 0;
        jlog ctx = open_reader(s);
        long start = ctx.raw_size();
        while(true) {
            jlog.Id chkpt, cur;
            jlog.Interval interval = ctx.read_interval();
            chkpt = cur = interval.getStart();
            log("reading interval(" + cur + "): " + interval);
            int i, batchsize = interval.count();
            if(batchsize == 0) break;
            for(i = 0; i < batchsize; i++) {
                if(i != 0) cur.increment();
                    @SuppressWarnings("unused")
                    jlog.Message m = ctx.read(cur);
                cnt++;
            }
            ctx.read_checkpoint(chkpt);
        }
        if(cnt != expect) fail("got wrong read count: " + cnt + " != " + expect);
        long end = ctx.raw_size();
        log("checking that size " + (sizeup ? "increased" : "decreased"));
        if(sizeup && (end < start)) fail("size didn't increase as exptected");
        if(!sizeup && (end > start)) fail("size didn't decrease as expected");
    }
 
    void addonefile(String fn, int i) throws Exception {
        String x = jlogpath + "/" + fn;
        PrintWriter pw = new PrintWriter(x);
        pw.write(payload, i*7, 7);
        pw.close();
    }
        
    void corruptmetastore() throws Exception {
        String x = jlogpath + "/metastore";
        PrintWriter pw = new PrintWriter(x);
        pw.write(payload, 0, 16);
        pw.close();
    }

    void addsomefiles() throws Exception {
        for(int i=0;i<names.length;i++)
            addonefile(names[i], i);
        corruptmetastore();
    }

    void test_repair(String s) throws Exception {
        log("Testing " + s);
        jlog ctx = new jlog(jlogpath);
        ctx.init();
        addsomefiles();
        int res = ctx.repair(0);
        String sres = (res == 0) ? "failed" : "succeeded";
        log("Repair " + sres);
    }
  
    public jlogTest(String[] called_args) {
        if(called_args.length > 0) jlogpath = called_args[0];
    }
          
    public void run() {
        try {
            initialize();
            test_subscriber("testing");
            assert_subscriber("testing");
            test_subscriber("witness");
            assert_subscriber("witness");
            try { assert_subscriber("badguy");
                fail("found badguy subscriber"); }
            catch (Exception e) {
                if(!e.getMessage().equals("nope")) throw e;
            }
            write_payloads(pcnt);
            read_check("witness", pcnt, true);
            read_check("testing", pcnt, false);
            test_repair("repair");
        } catch(Exception catchall) {
            catchall.printStackTrace();
        }
    }

    public static void main(String[] args) {
        (new jlogTest(args)).run();
    }
}
