//Copyright 2017 Jan Scherbaum
//
//Licensed under the Apache License, Version 2.0 (the "License");
//you may not use this file except in compliance with the License.
//You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
//Unless required by applicable law or agreed to in writing, software
//distributed under the License is distributed on an "AS IS" BASIS,
//WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//See the License for the specific language governing permissions and
//limitations under the License.

import za.co.absa.spline.persistence.api.PersistenceFactory
import scala.concurrent.Future
import za.co.absa.spline.persistence.api.DataLineageWriter
import za.co.absa.spline.model.DataLineage
import za.co.absa.spline.persistence.api.NopDataLineageReader
import scala.concurrent.ExecutionContext

class NullPersistenceFactory(conf: org.apache.commons.configuration.Configuration) extends PersistenceFactory(conf) {
  def createDataLineageReader(): za.co.absa.spline.persistence.api.DataLineageReader = new NopDataLineageReader
  def createDataLineageReaderOrGetDefault(default: za.co.absa.spline.persistence.api.DataLineageReader): za.co.absa.spline.persistence.api.DataLineageReader = createDataLineageReader()
 
  def createDataLineageWriter(): za.co.absa.spline.persistence.api.DataLineageWriter = {
    new DataLineageWriter {
      def store(lineage: DataLineage)(implicit x: ExecutionContext) = Future.successful()
    }
  }

}