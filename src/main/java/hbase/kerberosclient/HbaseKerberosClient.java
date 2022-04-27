package hbase.kerberosclient;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.security.UserGroupInformation;

public class HbaseKerberosClient {

    public static void main(String[] args) {
        try {
            //System.setProperty(CommonConstants.KRB_REALM, ConfigUtil.getProperty(CommonConstants.HADOOP_CONF, "krb.realm"));
            //System.setProperty(CommonConstants.KRB_KDC, ConfigUtil.getProperty(CommonConstants.HADOOP_CONF,"krb.kdc"));
            //System.setProperty(CommonConstants.KRB_DEBUG, "true");

            final Configuration config = HBaseConfiguration.create();

            //config.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION, AUTH_KRB);
            //config.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHORIZATION, AUTHORIZATION);
            //config.set(CommonConfigurationKeysPublic.FS_AUTOMATIC_CLOSE_KEY, AUTO_CLOSE);
            //config.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, defaultFS);
            config.set("hbase.zookeeper.quorum", "localhost");
            config.set("hbase.zookeeper.property.clientPort", "2181");
            config.set("hbase.security.authentication", "kerberos");
            config.set("hbase.security.authorization", "kerberos");
            config.set("hbase.cluster.distribute", "true");
            config.set("hbase.client.retries.number", Integer.toString(0));
            config.set("zookeeper.session.timeout", Integer.toString(6000));
            config.set("zookeeper.recovery.retry", Integer.toString(0));
            //config.set("hbase.master", "testuser");
            //config.set("zookeeper.znode.parent", "/hbase-secure");
            //config.set("hbase.rpc.engine", "org.apache.hadoop.hbase.ipc.SecureRpcEngine");
            config.set("hbase.master.kerberos.principal", "hbase/_HOST@xxx.com");
            config.set("hbase.master.keytab.file", "D:/keytab");
            config.set("hbase.regionserver.kerberos.principal", "hbase/_HOST@xxx.com");
            config.set("hbase.regionserver.keytab.file", "D:/keytab");

            UserGroupInformation.setConfiguration(config);
            UserGroupInformation userGroupInformation = UserGroupInformation.loginUserFromKeytabAndReturnUGI("testuser", "D:/keytab");
            UserGroupInformation.setLoginUser(userGroupInformation);


            Connection connection = ConnectionFactory.createConnection(HBaseConfiguration.create(config));
            System.out.println("connection: " +  connection.getConfiguration());

            Admin admin = connection.getAdmin();
            System.out.println("admin: " + admin.getMaster());
            TableName[] names = admin.listTableNames();
            System.out.println("names  size: " + names.length);
            for (TableName name : names) {
                System.out.println("name:\t" + name.getNameAsString());
            }
            System.out.println("UserGroupInformation UserName " + UserGroupInformation.getLoginUser().getUserName());


        } catch (Exception e) {
            e.printStackTrace();
        }

    }

}
