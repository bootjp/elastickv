import { Route, Routes } from "react-router-dom";
import { AuthProvider } from "./auth";
import { Layout } from "./components/Layout";
import { RequireAuth } from "./components/RequireAuth";
import { DashboardPage } from "./pages/Dashboard";
import { DynamoDetailPage } from "./pages/DynamoDetail";
import { DynamoListPage } from "./pages/DynamoList";
import { KeyVizPage } from "./pages/KeyViz";
import { LoginPage } from "./pages/Login";
import { NotFoundPage } from "./pages/NotFound";
import { S3DetailPage } from "./pages/S3Detail";
import { S3ListPage } from "./pages/S3List";
import { SqsDetailPage } from "./pages/SqsDetail";
import { SqsListPage } from "./pages/SqsList";

export function App() {
  return (
    <AuthProvider>
      <Routes>
        <Route path="/login" element={<LoginPage />} />
        <Route
          element={
            <RequireAuth>
              <Layout />
            </RequireAuth>
          }
        >
          <Route index element={<DashboardPage />} />
          <Route path="dynamo" element={<DynamoListPage />} />
          <Route path="dynamo/:name" element={<DynamoDetailPage />} />
          <Route path="sqs" element={<SqsListPage />} />
          <Route path="sqs/:name" element={<SqsDetailPage />} />
          <Route path="s3" element={<S3ListPage />} />
          <Route path="s3/:name" element={<S3DetailPage />} />
          <Route path="keyviz" element={<KeyVizPage />} />
          <Route path="*" element={<NotFoundPage />} />
        </Route>
      </Routes>
    </AuthProvider>
  );
}
