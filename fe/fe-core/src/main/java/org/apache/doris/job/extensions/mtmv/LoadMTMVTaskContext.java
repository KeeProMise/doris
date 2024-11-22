package org.apache.doris.job.extensions.mtmv;

import com.google.gson.annotations.SerializedName;
import org.apache.doris.nereids.trees.plans.commands.info.TableNameInfo;

import java.util.List;

public class LoadMTMVTaskContext extends MTMVTaskContext {
    @SerializedName(value = "table")
    private TableNameInfo table;
    @SerializedName("useTable")
    private boolean useTable;

    public LoadMTMVTaskContext(
        MTMVTask.MTMVTaskTriggerMode triggerMode, List<String> partitions,
        boolean isComplete, TableNameInfo table, boolean useTable) {
        super(triggerMode, partitions, isComplete);
        this.table = table;
        this.useTable = useTable;
    }

    public TableNameInfo getTable() {
        return table;
    }

    public boolean isUseTable() {
        return useTable;
    }

    @Override
    public String toString() {
        return "LoadMTMVTaskContext{" +
            "table=" + table +
            ", useTable=" + useTable +
            ", triggerMode=" + super.getTriggerMode()
            + ", partitions=" + super.getPartitions()
            + ", isComplete=" + super.isComplete()
            + '}';
    }
}
