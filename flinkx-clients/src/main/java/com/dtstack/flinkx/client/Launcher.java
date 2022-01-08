/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dtstack.flinkx.client;

import com.dtstack.flinkx.classloader.ClassLoaderManager;
import com.dtstack.flinkx.client.kubernetes.KubernetesApplicationClusterClientHelper;
import com.dtstack.flinkx.client.kubernetes.KubernetesSessionClusterClientHelper;
import com.dtstack.flinkx.client.local.LocalClusterClientHelper;
import com.dtstack.flinkx.client.standalone.StandaloneClusterClientHelper;
import com.dtstack.flinkx.client.yarn.YarnPerJobClusterClientHelper;
import com.dtstack.flinkx.client.yarn.YarnSessionClusterClientHelper;
import com.dtstack.flinkx.enums.ClusterMode;
import com.dtstack.flinkx.options.OptionParser;
import com.dtstack.flinkx.options.Options;
import com.dtstack.flinkx.util.ExecuteProcessHelper;
import com.dtstack.flinkx.util.JsonModifyUtil;

import org.apache.flink.client.deployment.ClusterDeploymentException;
import org.apache.flink.configuration.ConfigConstants;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.HashMap;
import java.util.List;

/**
 * FlinkX commandline Launcher
 *
 * <p>Company: www.dtstack.com
 *
 * @author huyifan.zju@163.com
 */
public class Launcher {
    // LOG没用到?不想打印日志就别写，西八
    private static final Logger LOG = LoggerFactory.getLogger(Launcher.class);

    public static final String KEY_FLINKX_HOME = "FLINKX_HOME";
    public static final String KEY_FLINK_HOME = "FLINK_HOME";
    public static final String KEY_HADOOP_HOME = "HADOOP_HOME";

    public static final String PLUGINS_DIR_NAME = "flinkx-dist";

    public static void main(String[] args) throws Exception {
        OptionParser optionParser = new OptionParser(args);
        // 使用Apache Common Cli解析程序参数
        Options launcherOptions = optionParser.getOptions();

        findDefaultConfigDir(launcherOptions);

        // 把OptionParser对象中的参数又转为list形式，一个参数名后面接着一个参数值，两两配对
        // 注意getProgramExeArgList方法检测到job选项后把文件路径替换为文件内容
        List<String> argList = optionParser.getProgramExeArgList();

        // 将argList转化为HashMap，方便通过参数名称来获取参数值
        // 那为什么getProgramExeArgList不直接返回Map？有点憨
        HashMap<String, String> temp = new HashMap<>(16);
        for (int i = 0; i < argList.size(); i += 2) {
            temp.put(argList.get(i), argList.get(i + 1));
        }

        // 对json中的变量值进行值替换
        String s = temp.get("-p");
        if (StringUtils.isNotBlank(s)) {
            // CommandTransform方法按照"参数名1=参数值1,参数名2=参数值2"的格式分割得到对应的HashMap
            HashMap<String, String> parameter = JsonModifyUtil.CommandTransform(s);
            // 根据变量参数替换json中的占位符，比如-p为name=kayhaw，json为{"name": "${name}"}，替换后
            // 得到{"name": "kayhaw"}
            // XXX 不喜欢这种replace函数替换字符串的方式，效率低(虽然从C语言过来的用着爽)
            // 为什么不一开始就让flinkx接收，最朴素直白的json配置，还要绕那么一圈弯，个人认为这种灵活性应该由
            // 调用flinnkx的服务来实现，而不是flinkx自己扩展
            temp.put("-job", JsonModifyUtil.JsonValueReplace(temp.get("-job"), parameter));
        }

        // 清空list，填充修改后的参数值，这argList和temp互相倒也是给爷整懵了
        argList.clear();
        for (int i = 0; i < temp.size(); i++) {
            argList.add(temp.keySet().toArray()[i].toString());
            argList.add(temp.values().toArray()[i].toString());
        }

        // argList其就是launcherOptions的字符串表示，这个参数设计冗余了
        JobDeployer jobDeployer = new JobDeployer(launcherOptions, argList);

        ClusterClientHelper clusterClientHelper = null;
        // 根据mode参数值构造对应的xxxClientHelper
        switch (ClusterMode.getByName(launcherOptions.getMode())) {
            case local:
                clusterClientHelper = new LocalClusterClientHelper();
                break;
            case standalone:
                clusterClientHelper = new StandaloneClusterClientHelper();
                break;
            case yarnSession:
                clusterClientHelper = new YarnSessionClusterClientHelper();
                break;
            case yarnPerJob:
                clusterClientHelper = new YarnPerJobClusterClientHelper();
                break;
            case yarnApplication:
                throw new ClusterDeploymentException(
                        "Application Mode not supported by Yarn deployments.");
            case kubernetesSession:
                clusterClientHelper = new KubernetesSessionClusterClientHelper();
                break;
            case kubernetesPerJob:
                throw new ClusterDeploymentException(
                        "Per-Job Mode not supported by Kubernetes deployments.");
            case kubernetesApplication:
                clusterClientHelper = new KubernetesApplicationClusterClientHelper();
                break;
            default:
                throw new ClusterDeploymentException(
                        launcherOptions.getMode() + " Mode not supported.");
        }

        // add ext class
        URLClassLoader urlClassLoader = (URLClassLoader) Launcher.class.getClassLoader();
        // add jar是UDF jar包，没有用到的话跳过
        List<URL> jarUrlList = ExecuteProcessHelper.getExternalJarUrls(launcherOptions.getAddjar());
        ClassLoaderManager.loadExtraJar(jarUrlList, urlClassLoader);
        clusterClientHelper.submit(jobDeployer);
    }

    private static void findDefaultConfigDir(Options launcherOptions) {
        findDefaultFlinkxDistDir(launcherOptions);

        if (ClusterMode.local.name().equalsIgnoreCase(launcherOptions.getMode())) {
            return;
        }

        findDefaultFlinkConf(launcherOptions);
        findDefaultHadoopConf(launcherOptions);
    }

    private static void findDefaultHadoopConf(Options launcherOptions) {
        if (StringUtils.isNotEmpty(launcherOptions.getHadoopConfDir())) {
            return;
        }

        String hadoopHome = getSystemProperty(KEY_HADOOP_HOME);
        if (StringUtils.isNotEmpty(hadoopHome)) {
            hadoopHome = hadoopHome.trim();
            if (hadoopHome.endsWith(File.separator)) {
                hadoopHome = hadoopHome.substring(0, hadoopHome.lastIndexOf(File.separator));
            }

            launcherOptions.setHadoopConfDir(hadoopHome + "/etc/hadoop");
        }
    }

    private static void findDefaultFlinkConf(Options launcherOptions) {

        String flinkHome = getSystemProperty(KEY_FLINK_HOME);
        if (StringUtils.isNotEmpty(flinkHome)) {
            flinkHome = flinkHome.trim();
            if (flinkHome.endsWith(File.separator)) {
                flinkHome = flinkHome.substring(0, flinkHome.lastIndexOf(File.separator));
            }

            if (StringUtils.isEmpty(launcherOptions.getFlinkConfDir())) {
                launcherOptions.setFlinkConfDir(flinkHome + "/conf");
            }

            if (StringUtils.isEmpty(launcherOptions.getFlinkLibDir())) {
                launcherOptions.setFlinkLibDir(flinkHome + "/lib");
            }
        }
    }

    private static void findDefaultFlinkxDistDir(Options launcherOptions) {
        String distDir = launcherOptions.getFlinkxDistDir();
        if (StringUtils.isEmpty(distDir)) {
            String flinkxHome = getSystemProperty(KEY_FLINKX_HOME);
            if (StringUtils.isNotEmpty(flinkxHome)) {
                flinkxHome = flinkxHome.trim();
                if (flinkxHome.endsWith(File.separator)) {
                    distDir = flinkxHome + PLUGINS_DIR_NAME;
                } else {
                    distDir = flinkxHome + File.separator + PLUGINS_DIR_NAME;
                }

                launcherOptions.setFlinkxDistDir(distDir);
            }
        }
        System.setProperty(ConfigConstants.ENV_FLINK_PLUGINS_DIR, distDir);
    }

    private static String getSystemProperty(String name) {
        String property = System.getenv(name);
        if (StringUtils.isEmpty(property)) {
            property = System.getProperty(name);
        }

        return property;
    }
}
