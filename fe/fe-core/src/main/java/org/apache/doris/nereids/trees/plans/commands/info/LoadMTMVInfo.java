package org.apache.doris.nereids.trees.plans.commands.info;

import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.qe.ConnectContext;

import java.util.List;

public class LoadMTMVInfo extends RefreshMTMVInfo{
    private TableNameInfo table;
    private boolean useTable;

    public LoadMTMVInfo(
        TableNameInfo mvName, List<String> partitions, boolean isComplete, TableNameInfo table, boolean useBase) {
        super(mvName, partitions, isComplete);
        this.table = table;
        this.useTable = useBase;
    }

    @Override
    public void analyze(ConnectContext ctx) {
        super.analyze(ctx);
        if (!Env.getCurrentEnv().getAccessManager().checkTblPriv(ctx, table.getCtl(), table.getDb(),
            table.getTbl(), PrivPredicate.LOAD)) {
            String message = ErrorCode.ERR_TABLEACCESS_DENIED_ERROR.formatErrorMsg("CREATE",
                ctx.getQualifiedUser(), ctx.getRemoteIP(),
                table.getDb() + ": " + table.getTbl());
            throw new AnalysisException(message);
        }
        try {
            Database db = Env.getCurrentInternalCatalog().getDbOrDdlException(table.getDb());
            db.getTableOrMetaException(table.getTbl(), TableIf.TableType.OLAP);
        } catch (MetaNotFoundException | DdlException e) {
            throw new AnalysisException(e.getMessage());
        }
    }

    public TableNameInfo getTable() {
        return table;
    }

    public boolean isUseTable() {
        return useTable;
    }

    @Override
    public String toString() {
        return "LoadMMTMVInfo{" +
            "table=" + table +
            ", useTable=" + useTable +
            ", mvName=" + getMvName() +
            ", partitions=" + getPartitions() +
            ", isComplete=" + isComplete() +
            '}';
    }
}
