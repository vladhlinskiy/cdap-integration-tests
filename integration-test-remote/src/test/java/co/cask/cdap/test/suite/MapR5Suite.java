/*
 * Copyright © 2015 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap.test.suite;

import co.cask.cdap.test.suite.category.MapR5Incompatible;
import co.cask.cdap.test.suite.category.RequiresSpark;
import org.junit.experimental.categories.Categories;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * Junit suite for tests that should run on MapR 5.x.
 */
@RunWith(Categories.class)
// coopr doesn't provision MapR cluster with Spark. Remove exclusion of "RequiresSpark" once COOK-108 is fixed
@Categories.ExcludeCategory({MapR5Incompatible.class, RequiresSpark.class})
@Suite.SuiteClasses({
  AllTests.class
})
public class MapR5Suite {
}
