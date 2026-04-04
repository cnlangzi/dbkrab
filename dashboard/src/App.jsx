import React, { useState, useEffect } from 'react';
import { Layout, Menu, Card, Statistic, Table, Tag, Space, Button, message, Modal, Form, Input, Descriptions, Badge, Alert } from 'antd';
import {
  DashboardOutlined,
  DatabaseOutlined,
  AppstoreOutlined,
  CheckCircleOutlined,
  CloseCircleOutlined,
  SyncOutlined,
  DeleteOutlined,
  EyeOutlined,
  ReloadOutlined,
} from '@ant-design/icons';

const { Header, Content, Sider } = Layout;

// API base URL
const API_BASE = '/api';

// Main App Component
function App() {
  const [collapsed, setCollapsed] = useState(false);
  const [activeTab, setActiveTab] = useState('overview');

  return (
    <Layout style={{ minHeight: '100vh' }}>
      <Sider collapsible collapsed={collapsed} onCollapse={setCollapsed} theme="dark">
        <div style={{ 
          height: 32, 
          margin: 16, 
          background: 'rgba(255, 255, 255, 0.2)',
          borderRadius: 4,
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'center',
          color: '#fff',
          fontWeight: 'bold',
        }}>
          {collapsed ? '🦀' : '🦀 dbkrab'}
        </div>
        <Menu
          theme="dark"
          mode="inline"
          selectedKeys={[activeTab]}
          items={[
            {
              key: 'overview',
              icon: <DashboardOutlined />,
              label: 'Overview',
              onClick: () => setActiveTab('overview'),
            },
            {
              key: 'dlq',
              icon: <DatabaseOutlined />,
              label: 'Dead Letter Queue',
              onClick: () => setActiveTab('dlq'),
            },
            {
              key: 'plugins',
              icon: <AppstoreOutlined />,
              label: 'Plugins',
              onClick: () => setActiveTab('plugins'),
            },
          ]}
        />
      </Sider>
      <Layout>
        <Header style={{ 
          padding: '0 24px', 
          background: '#fff',
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'space-between',
          boxShadow: '0 1px 4px rgba(0,21,41,.08)',
        }}>
          <h2 style={{ margin: 0 }}>
            {activeTab === 'overview' && 'Overview'}
            {activeTab === 'dlq' && 'Dead Letter Queue'}
            {activeTab === 'plugins' && 'Plugins Management'}
          </h2>
          <Badge count="v1.0" style={{ backgroundColor: '#52c41a' }} />
        </Header>
        <Content style={{ margin: '24px 16px', padding: 24, background: '#fff' }}>
          {activeTab === 'overview' && <Overview />}
          {activeTab === 'dlq' && <DLQDashboard />}
          {activeTab === 'plugins' && <PluginsDashboard />}
        </Content>
      </Layout>
    </Layout>
  );
}

// Overview Dashboard
function Overview() {
  const [stats, setStats] = useState({ dlq: { pending: 0, resolved: 0, ignored: 0 }, plugins: 0 });
  const [loading, setLoading] = useState(true);
  const [health, setHealth] = useState('unknown');

  useEffect(() => {
    fetchStats();
    checkHealth();
    const interval = setInterval(() => {
      fetchStats();
      checkHealth();
    }, 10000);
    return () => clearInterval(interval);
  }, []);

  const fetchStats = async () => {
    try {
      const [dlqRes, pluginsRes] = await Promise.all([
        fetch(`${API_BASE}/dlq/stats`),
        fetch(`${API_BASE}/plugins`),
      ]);
      const dlqData = await dlqRes.json();
      const pluginsData = await pluginsRes.json();
      
      const dlqStats = { pending: 0, resolved: 0, ignored: 0 };
      if (dlqData.success && dlqData.stats) {
        dlqData.stats.forEach(s => {
          dlqStats[s.status] = s.count;
        });
      }
      
      setStats({
        dlq: dlqStats,
        plugins: pluginsData.plugins?.length || 0,
      });
    } catch (error) {
      console.error('Failed to fetch stats:', error);
    } finally {
      setLoading(false);
    }
  };

  const checkHealth = async () => {
    try {
      const res = await fetch(`${API_BASE}/health`);
      setHealth(res.ok ? 'healthy' : 'unhealthy');
    } catch {
      setHealth('unhealthy');
    }
  };

  return (
    <Space direction="vertical" size="large" style={{ width: '100%' }}>
      <Alert
        message={health === 'healthy' ? 'System Healthy' : 'System Unhealthy'}
        type={health === 'healthy' ? 'success' : 'error'}
        showIcon
        icon={health === 'healthy' ? <CheckCircleOutlined /> : <CloseCircleOutlined />}
      />
      
      <Space size="large" wrap>
        <Card style={{ minWidth: 200 }}>
          <Statistic
            title="DLQ Pending"
            value={stats.dlq.pending}
            valueStyle={{ color: stats.dlq.pending > 0 ? '#cf1322' : '#3f8600' }}
            prefix={stats.dlq.pending > 0 ? <CloseCircleOutlined /> : <CheckCircleOutlined />}
          />
        </Card>
        <Card style={{ minWidth: 200 }}>
          <Statistic
            title="DLQ Resolved"
            value={stats.dlq.resolved}
            valueStyle={{ color: '#3f8600' }}
            prefix={<CheckCircleOutlined />}
          />
        </Card>
        <Card style={{ minWidth: 200 }}>
          <Statistic
            title="DLQ Ignored"
            value={stats.dlq.ignored}
            valueStyle={{ color: '#8c8c8c' }}
            prefix={<SyncOutlined />}
          />
        </Card>
        <Card style={{ minWidth: 200 }}>
          <Statistic
            title="Active Plugins"
            value={stats.plugins}
            valueStyle={{ color: '#1890ff' }}
            prefix={<AppstoreOutlined />}
          />
        </Card>
      </Space>

      <Card title="Quick Actions">
        <Space>
          <Button icon={<ReloadOutlined />} onClick={() => window.location.reload()}>
            Refresh Dashboard
          </Button>
        </Space>
      </Card>
    </Space>
  );
}

// DLQ Dashboard
function DLQDashboard() {
  const [entries, setEntries] = useState([]);
  const [loading, setLoading] = useState(false);
  const [selectedEntry, setSelectedEntry] = useState(null);
  const [modalVisible, setModalVisible] = useState(false);
  const [ignoreForm] = Form.useForm();

  useEffect(() => {
    fetchEntries();
  }, []);

  const fetchEntries = async () => {
    setLoading(true);
    try {
      const res = await fetch(`${API_BASE}/dlq/list`);
      const data = await res.json();
      if (data.success) {
        setEntries(data.entries || []);
      }
    } catch (error) {
      message.error('Failed to fetch DLQ entries');
    } finally {
      setLoading(false);
    }
  };

  const handleReplay = async (id) => {
    try {
      const res = await fetch(`${API_BASE}/dlq/${id}/replay`, { method: 'POST' });
      const data = await res.json();
      if (data.success) {
        message.success('Entry replayed successfully');
        fetchEntries();
      } else {
        message.error(data.error || 'Failed to replay');
      }
    } catch {
      message.error('Failed to replay');
    }
  };

  const handleIgnore = async (id, note) => {
    try {
      const res = await fetch(`${API_BASE}/dlq/${id}/ignore`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ note }),
      });
      const data = await res.json();
      if (data.success) {
        message.success('Entry ignored');
        setModalVisible(false);
        ignoreForm.resetFields();
        fetchEntries();
      } else {
        message.error(data.error || 'Failed to ignore');
      }
    } catch {
      message.error('Failed to ignore');
    }
  };

  const handleDelete = async (id) => {
    Modal.confirm({
      title: 'Delete Entry',
      content: 'Are you sure you want to delete this entry?',
      onOk: async () => {
        try {
          const res = await fetch(`${API_BASE}/dlq/${id}`, { method: 'DELETE' });
          const data = await res.json();
          if (data.success) {
            message.success('Entry deleted');
            fetchEntries();
          } else {
            message.error(data.error || 'Failed to delete');
          }
        } catch {
          message.error('Failed to delete');
        }
      },
    });
  };

  const columns = [
    {
      title: 'ID',
      dataIndex: 'id',
      key: 'id',
      width: 60,
    },
    {
      title: 'Table',
      dataIndex: 'table',
      key: 'table',
    },
    {
      title: 'LSN',
      dataIndex: 'lsn',
      key: 'lsn',
    },
    {
      title: 'Error',
      dataIndex: 'error',
      key: 'error',
      ellipsis: true,
    },
    {
      title: 'Status',
      dataIndex: 'status',
      key: 'status',
      render: (status) => {
        const colors = {
          pending: 'red',
          resolved: 'green',
          ignored: 'gray',
        };
        return <Tag color={colors[status]}>{status}</Tag>;
      },
    },
    {
      title: 'Created At',
      dataIndex: 'created_at',
      key: 'created_at',
      render: (ts) => ts ? new Date(ts).toLocaleString() : '-',
    },
    {
      title: 'Actions',
      key: 'actions',
      render: (_, record) => (
        <Space>
          <Button
            size="small"
            icon={<EyeOutlined />}
            onClick={() => {
              setSelectedEntry(record);
              setModalVisible(true);
            }}
          >
            View
          </Button>
          {record.status === 'pending' && (
            <>
              <Button
                size="small"
                type="primary"
                icon={<ReloadOutlined />}
                onClick={() => handleReplay(record.id)}
              >
                Replay
              </Button>
              <Button
                size="small"
                icon={<SyncOutlined />}
                onClick={() => {
                  setSelectedEntry(record);
                  setModalVisible(true);
                }}
              >
                Ignore
              </Button>
            </>
          )}
          <Button
            size="small"
            danger
            icon={<DeleteOutlined />}
            onClick={() => handleDelete(record.id)}
          >
            Delete
          </Button>
        </Space>
      ),
    },
  ];

  return (
    <Space direction="vertical" size="large" style={{ width: '100%' }}>
      <Space>
        <Button icon={<ReloadOutlined />} onClick={fetchEntries} loading={loading}>
          Refresh
        </Button>
      </Space>

      <Table
        columns={columns}
        dataSource={entries}
        loading={loading}
        rowKey="id"
        pagination={{ pageSize: 20 }}
      />

      <Modal
        title={
          <Space>
            {selectedEntry?.status === 'pending' ? 'Ignore' : 'View'} Entry #{selectedEntry?.id}
          </Space>
        }
        open={modalVisible}
        onCancel={() => {
          setModalVisible(false);
          ignoreForm.resetFields();
        }}
        footer={
          selectedEntry?.status === 'pending'
            ? [
                <Button key="cancel" onClick={() => setModalVisible(false)}>
                  Cancel
                </Button>,
                <Button
                  key="ignore"
                  type="primary"
                  danger
                  onClick={() => {
                    ignoreForm.validateFields().then(({ note }) => {
                      handleIgnore(selectedEntry.id, note);
                    });
                  }}
                >
                  Ignore
                </Button>,
              ]
            : [
                <Button key="close" onClick={() => setModalVisible(false)}>
                  Close
                </Button>,
              ]
        }
      >
        {selectedEntry && (
          <Space direction="vertical" size="large" style={{ width: '100%' }}>
            <Descriptions column={1} bordered>
              <Descriptions.Item label="ID">{selectedEntry.id}</Descriptions.Item>
              <Descriptions.Item label="Table">{selectedEntry.table}</Descriptions.Item>
              <Descriptions.Item label="LSN">{selectedEntry.lsn}</Descriptions.Item>
              <Descriptions.Item label="Status">
                <Tag>{selectedEntry.status}</Tag>
              </Descriptions.Item>
              <Descriptions.Item label="Error">{selectedEntry.error}</Descriptions.Item>
              <Descriptions.Item label="Created At">
                {selectedEntry.created_at ? new Date(selectedEntry.created_at).toLocaleString() : '-'}
              </Descriptions.Item>
              <Descriptions.Item label="Data">
                <pre style={{ 
                  background: '#f5f5f5', 
                  padding: 12, 
                  borderRadius: 4, 
                  maxHeight: 300, 
                  overflow: 'auto',
                  fontSize: 12,
                }}>
                  {JSON.stringify(selectedEntry.data, null, 2)}
                </pre>
              </Descriptions.Item>
            </Descriptions>

            {selectedEntry.status === 'pending' && (
              <Form form={ignoreForm} layout="vertical">
                <Form.Item
                  name="note"
                  label="Reason for ignoring"
                  rules={[{ required: true, message: 'Please enter a reason' }]}
                >
                  <Input.TextArea rows={3} placeholder="Why are you ignoring this entry?" />
                </Form.Item>
              </Form>
            )}
          </Space>
        )}
      </Modal>
    </Space>
  );
}

// Plugins Dashboard
function PluginsDashboard() {
  const [plugins, setPlugins] = useState([]);
  const [loading, setLoading] = useState(false);

  useEffect(() => {
    fetchPlugins();
  }, []);

  const fetchPlugins = async () => {
    setLoading(true);
    try {
      const res = await fetch(`${API_BASE}/plugins`);
      const data = await res.json();
      if (data.success) {
        setPlugins(data.plugins || []);
      }
    } catch {
      message.error('Failed to fetch plugins');
    } finally {
      setLoading(false);
    }
  };

  const handleReload = async (name) => {
    try {
      const res = await fetch(`${API_BASE}/plugins/${name}/reload`, { method: 'POST' });
      const data = await res.json();
      if (data.success) {
        message.success(`Plugin ${name} reloaded`);
        fetchPlugins();
      } else {
        message.error(data.error || 'Failed to reload');
      }
    } catch {
      message.error('Failed to reload');
    }
  };

  const handleUnload = async (name) => {
    Modal.confirm({
      title: 'Unload Plugin',
      content: `Are you sure you want to unload plugin "${name}"?`,
      onOk: async () => {
        try {
          const res = await fetch(`${API_BASE}/plugins/${name}`, { method: 'DELETE' });
          const data = await res.json();
          if (data.success) {
            message.success(`Plugin ${name} unloaded`);
            fetchPlugins();
          } else {
            message.error(data.error || 'Failed to unload');
          }
        } catch {
          message.error('Failed to unload');
        }
      },
    });
  };

  const columns = [
    {
      title: 'Name',
      dataIndex: 'name',
      key: 'name',
    },
    {
      title: 'Description',
      dataIndex: 'description',
      key: 'description',
    },
    {
      title: 'Version',
      dataIndex: 'version',
      key: 'version',
    },
    {
      title: 'Tables',
      dataIndex: 'tables',
      key: 'tables',
      render: (tables) => tables?.join(', ') || '-',
    },
    {
      title: 'Actions',
      key: 'actions',
      render: (_, record) => (
        <Space>
          <Button
            size="small"
            icon={<ReloadOutlined />}
            onClick={() => handleReload(record.name)}
          >
            Reload
          </Button>
          <Button
            size="small"
            danger
            onClick={() => handleUnload(record.name)}
          >
            Unload
          </Button>
        </Space>
      ),
    },
  ];

  return (
    <Space direction="vertical" size="large" style={{ width: '100%' }}>
      <Space>
        <Button icon={<ReloadOutlined />} onClick={fetchPlugins} loading={loading}>
          Refresh
        </Button>
      </Space>

      <Table
        columns={columns}
        dataSource={plugins}
        loading={loading}
        rowKey="name"
        pagination={false}
      />
    </Space>
  );
}

export default App;
