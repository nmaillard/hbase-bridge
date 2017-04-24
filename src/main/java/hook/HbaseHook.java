package hook;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;

import org.apache.atlas.hook.AtlasHook;
import org.apache.atlas.notification.hook.HookNotification;
import org.apache.atlas.typesystem.Referenceable;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.coprocessor.BaseMasterObserver;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;

public class HbaseHook extends BaseMasterObserver{
	public static final Log LOG = LogFactory.getLog(HbaseHook.class);
	private static ExecutorService executor = null;
	
	
	
	@Override
	public void postCreateTable (ObserverContext<MasterCoprocessorEnvironment> c, HTableDescriptor desc, HRegionInfo[] regions){
		List<Referenceable> lc = new ArrayList<Referenceable>();
		Referenceable tableRef = createTableInstance(desc,desc.getTableName());
   	 	lc.add(tableRef);
   	 	for(HColumnDescriptor hc:desc.getColumnFamilies()){
   	 		lc.add(createColumnFamilyInstance(tableRef,hc));
   	 	}    
   	 	HookNotification.HookNotificationMessage message =
             new HookNotification.EntityCreateRequest("nico",lc);
   	 AtlasHook.notifyEntities(Arrays.asList(message), 3);
	}
	
	@Override
	public void postCreateNamespace(ObserverContext<MasterCoprocessorEnvironment> ctx, NamespaceDescriptor ns){
		LOG.info(" Hbase Namespace not yet supported by Hbase model ");
		
	}
	
	@Override
	public void postDeleteTable(ObserverContext<MasterCoprocessorEnvironment> c, TableName tableName){
        final String tblQualifiedName = getTableQualifiedName(tableName);
        LOG.info("Deleting hbase table {} ");
        HookNotification.HookNotificationMessage message = new HookNotification.EntityDeleteRequest("nico", "hbase_table", "qualifiedName", tblQualifiedName);
        AtlasHook.notifyEntities(Arrays.asList(message), 3);
	}
	
	@Override
	public void postDeleteNamespace(ObserverContext<MasterCoprocessorEnvironment> ctx, String namespace){
	}
	
	public void postCompletedAddColumnFamilyAction(ObserverContext<MasterCoprocessorEnvironment> ctx, TableName tableName, HColumnDescriptor columnFamily){
	}
	
	public void 	postCompletedDeleteColumnFamilyAction(ObserverContext<MasterCoprocessorEnvironment> ctx, TableName tableName, byte[] columnFamily){
	}
	
	private static Referenceable createColumnFamilyInstance(Referenceable tableRef, HColumnDescriptor hc){
		Referenceable Cfref = new Referenceable("hbase_column_family");
		Cfref.set("name", hc.getNameAsString());
		Cfref.set("table", tableRef);
		Cfref.set("qualifiedName",getColumFamilyQualifiedName(tableRef.get("qualifiedName").toString(),hc));
		Cfref.set("owner", tableRef.get("owner"));
		Cfref.set("description", hc.toString());
		return Cfref;
		
	}
	
    private static Referenceable createTableInstance(HTableDescriptor desc, TableName tableName){
    	Referenceable tableRef = new Referenceable("hbase_table");
    	tableRef.set("qualifiedName",getTableQualifiedName(tableName));
        tableRef.set("name",tableName.getNameAsString() );
        tableRef.set("namespace", tableName.getNamespaceAsString());
        tableRef.set("owner", desc.getOwnerString());
        tableRef.set("description", tableName.toString());
        tableRef.set("uri", "my uri");
    	return tableRef;
		
	}
    
    private static String getTableQualifiedName(TableName tableName){
    	return tableName.getNamespaceAsString()+":"+tableName.getNameAsString()+"@hDP26";
    }
    
    private static String getColumFamilyQualifiedName(String tableQualifiedName,HColumnDescriptor hc){
    	final String[] parts = tableQualifiedName.split("@");
        final String tableName = parts[0];
        final String clusterName = parts[1];
    	return String.format("%s.%s@%s", tableName, hc.getNameAsString(), clusterName);
    }

}
